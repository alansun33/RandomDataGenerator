using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace RandomEntityGenerator
{
    public class ModelFactory
    {
        public static void AppendNewDataTableRow(DataTable dt, List<string> pk, Dictionary<string, DataTable> fkDict,
            List<string> selfJoinKeys)
        {
            var row = dt.NewRow();
            var ordinals = new Type[dt.Columns.Count];
            var pkCols = new List<int>();
            var fkCols = new List<int>();
            var sjCols = new List<int>();
            for (var i = 0; i < ordinals.Length; i++)
            {
                ordinals[i] = dt.Columns[i].DataType;
                //Primary Key and Self Join Foreign Key
                if (pk.Contains(dt.Columns[i].ColumnName))
                {
                    pkCols.Add(i);
                }
                if (fkDict.Keys.Contains(dt.Columns[i].ColumnName))
                {
                    fkCols.Add(i);
                }
                if (selfJoinKeys != null && selfJoinKeys.Contains(dt.Columns[i].ColumnName))
                {
                    sjCols.Add(i);
                }
            }

            //TODO: This might cause 
            Parallel.For(0, ordinals.Length, (i) =>
            {
                if (pkCols.Contains(i))
                {
                    if (ordinals[i] == typeof(Guid))
                    {
                        row[i] = Guid.NewGuid();
                    }
                }
                else if (fkCols.Contains(i))
                {
                    var fkColName = dt.Columns[i].ColumnName;
                    var dataTable = fkDict[fkColName];
                    var randomRowNum = RandomValueFactory.GetRandomInteger(0, dataTable.Rows.Count);
                    var alia = DBHelper.ColumnAliasMap.ContainsKey(fkColName)
                        ? DBHelper.ColumnAliasMap[fkColName]
                        : fkColName;
                    row[i] = dataTable.Rows[randomRowNum][alia];
                }
                else
                {
                    //TODO: This is hard coded in case of self join on int Primary Key
                    var length = sjCols.Contains(i) ? dt.Rows.Count : 10000;
                    row[i] = RandomValueFactory.GetRandomValue(ordinals[i], max: length);
                }
            });
            dt.Rows.Add(row);
        }
    }
}
