using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace RandomEntityGenerator
{
    public class DBHelper
    {
        public static Dictionary<string, string> ColumnAliasMap = new Dictionary<string, string>();
        public static Dictionary<string, string> ForeignKeyAndRefTableMap = new Dictionary<string, string>();

        public static DataTable GetEmptyDataTable(string table)
        {
            string query = "SELECT TOP 0 * FROM dbo.[" + table + "]";
            return ExecuteSqlQuery(query).Result;
        }

        /// <summary>
        /// Get ForeignKey -> MasterTable map and ForeignKeyAlia -> master Table ColumnName map
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public static void GetForeignKeyMap(string table)
        {
            var dt = GetForeignConstraints(table).Result;
            var rows = dt.Select(@"[TableName] <> [ReferenceTableName]");
            ColumnAliasMap = rows.ToDictionary(r => r["ColName"].ToString(), r => r["RefColName"].ToString());
            ForeignKeyAndRefTableMap = rows.ToDictionary(r => r["ColName"].ToString(), r => r["ReferenceTableName"].ToString());
        }

        public static List<string> GetSelfJoinKeys(string table)
        {
            var dt = GetForeignConstraints(table).Result;
            var rows = dt.Select(@"[TableName] = [ReferenceTableName]");
            return rows.Select(r => r["ColName"].ToString()).ToList();
        }

        public static async Task<DataTable> GetPrimaryConstraints(string table)
        {
            string query = "SELECT OBJECT_NAME(k.parent_object_id) TableName, " +
                           "c.COLUMN_NAME PrimaryKeyColumnName " +
                           "FROM sys.key_constraints k " +
                           "INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE c on c.CONSTRAINT_NAME = k.name " +
                           "WHERE OBJECT_NAME(k.parent_object_id) = '" + table + "'";
            return await ExecuteSqlQuery(query);
        }

        public static async Task<DataTable> GetForeignConstraints(string table)
        {
            string query = "SELECT OBJECT_NAME(f.parent_object_id) TableName, " +
                           "COL_NAME(f.parent_object_id, fc.parent_column_id) ColName, " +
                           "OBJECT_NAME(f.referenced_object_id) ReferenceTableName, " +
                           "COL_NAME(f.referenced_object_id, fc.referenced_column_id) RefColName " +
                           "FROM sys.foreign_keys f " +
                           "INNER JOIN sys.foreign_key_columns fc on f.object_id = fc.constraint_object_id " +
                           "WHERE OBJECT_NAME(f.parent_object_id) = '" + table + "'";
            return await ExecuteSqlQuery(query);
        }

        /// <summary>
        /// Send DataTable to Database
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        public static async Task SendToDatabase(DataTable dt, string table)
        {
            if (string.IsNullOrEmpty(table))
            {
                return;
            }
            using (var sqlConn = new SqlConnection(Config.SqlConnectionString))
            using (var copy = new SqlBulkCopy(sqlConn))
            {
                copy.BulkCopyTimeout = 600;
                copy.DestinationTableName = "dbo.[" + table + "]";
                sqlConn.Open();
                await copy.WriteToServerAsync(dt);
            }
        }

        /// <summary>
        /// Generate DataTable that could be used to update corresponding DB table
        /// </summary>
        /// <param name="table">DB table name</param>
        /// <param name="rows">Num of rows that will be populated</param>
        /// <returns></returns>
        public static async Task<DataTable> PopulateDataTable(string table, int rows)
        {
            var dt = GetEmptyDataTable(table);
            GetForeignKeyMap(table);
            var primaryKeys = await GetOriginalPrimaryKey(table);
            var referenceDict = new Dictionary<string, DataTable>();

            if (ForeignKeyAndRefTableMap.Count != 0)
            {
                await GetForeignReferences(ForeignKeyAndRefTableMap, referenceDict);
            }

            var selfJoinList = GetSelfJoinKeys(table);

            for (var i = 0; i < rows; i++)
            {
                ModelFactory.AppendNewDataTableRow(dt, primaryKeys, referenceDict, selfJoinList);
            }
            return dt;
        }

        private static async Task GetForeignReferences(Dictionary<string, string> fkMaps, Dictionary<string, DataTable> referenceDict)
        {
            foreach (var fk in fkMaps.Keys)
            {
                var alia = ColumnAliasMap.ContainsKey(fk) ? ColumnAliasMap[fk] : fk;
                var masterTable = await GetReferenceValuesFromMasterTable(alia, fkMaps[fk]);
                //Reference to other table
                if (masterTable != null && masterTable.Rows.Count != 0)
                {
                    referenceDict[fk] = masterTable;
                    continue;
                }
                throw new DataException("Table [" + fkMaps[fk] + "] has no data");
            }
        }

        public static async Task<DataTable> GetReferenceValuesFromMasterTable(string foreignKey, string masterTable)
        {
            string query = "SELECT [" + foreignKey +"] FROM [" + masterTable + "]";
            return await ExecuteSqlQuery(query);
        }

        private static async Task<List<string>> GetOriginalPrimaryKey(string table)
        {
            var pks = await GetPrimaryConstraints(table);
            var fks = await GetForeignConstraints(table);
            var fkCols = fks.Rows.Cast<DataRow>().Select(r => r["ColName"]).ToList();
            return pks.Rows.Cast<DataRow>().Select(r => r["PrimaryKeyColumnName"].ToString()).Where(c => !fkCols.Contains(c)).ToList();
        }

        private static async Task<DataTable> ExecuteSqlQuery(string query)
        {
            DataTable dt = new DataTable();
            using (SqlConnection sqlConn = new SqlConnection(Config.SqlConnectionString))
            using (SqlCommand cmd = new SqlCommand(query, sqlConn))
            {
                sqlConn.Open();
                dt.Load(await cmd.ExecuteReaderAsync());
            }
            return dt;
        }
    }
}