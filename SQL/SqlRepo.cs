using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;
using Unreal.Data.Interface;
using Unreal.Data.Linq;
using Dapper;
using System.Linq.Expressions;

namespace Unreal.Data.SQL
{
    [AttributeUsage(AttributeTargets.Class)]
    public class TableNameAttribute : Attribute
    {
        public string TableName { get; set; }
        public TableNameAttribute(string tableName)
        {
            TableName = tableName;
        }
    }

    [AttributeUsage(AttributeTargets.Property)]
    public class KeyAttribute : Attribute
    {
        public KeyAttribute(bool autoincrement = true)
        {
            Autoincrement = autoincrement;
        }
        public bool Autoincrement { get; set; }
    }

    public class ColumnInfo<T>
    {
        public PropertyInfo Property { get; set; }

        public string ColumnName { get; set; }

        public bool Key { get; set; }

        public bool Autoincrement { get; set; }

        public Func<T, object> GetterDelegate { get; set; }

        public Action<T, object> SetterDelegate { get; set; }
    }

    public class EntityContext<T>
    {
        public static string TableName { get; private set; }
        public static ColumnInfo<T> Key { get; private set; }
        public static IList<ColumnInfo<T>> Columns { get; private set; }

        public static Type Type { get; private set; }

        private static object _lock = new object();

        static EntityContext()
        {
            Type = typeof(T);
            TableName = GetTableName();
            Columns = GetColumns();
        }

        private static string GetTableName()
        {
            var tableNameAttr = Type.GetCustomAttribute<TableNameAttribute>();
            if (tableNameAttr == null)
                return Type.Name;
            return tableNameAttr.TableName;
        }

        private static IList<ColumnInfo<T>> GetColumns()
        {
            var columns = new List<ColumnInfo<T>>();
            var properties = Type.GetProperties();
            foreach (var p in properties)
            {
                if (null == p.GetGetMethod() || null == p.GetSetMethod()) continue;
                var ci = new ColumnInfo<T>() { Property = p };
                ci.ColumnName = p.Name;
                var keyAttr = p.GetCustomAttribute<KeyAttribute>();
                if (keyAttr != null)
                {
                    ci.Key = true;
                    if (keyAttr.Autoincrement)
                        ci.Autoincrement = true;
                }
                ci.GetterDelegate = CreateGetterDelegate(p);
                ci.SetterDelegate = CreateSetterDelegate(p);
                if (ci.Key)
                    Key = ci;
                columns.Add(ci);
            }
            if (Key == null)
            {
                var id = columns.FirstOrDefault(c => c.Property.Name.ToLower() == "id");
                if (id != null)
                {
                    id.Key = true;
                    id.Autoincrement = true;
                    Key = id;
                }
            }
            return columns;
        }

        private static Func<T, object> CreateGetterDelegate(PropertyInfo info)
        {
            var dynamicMethod = new DynamicMethod(Guid.NewGuid().ToString("N") + "_GetterDelegate", typeof(object), new[] { typeof(T) });
            var il = dynamicMethod.GetILGenerator();
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Callvirt, info.GetGetMethod());
            if (info.PropertyType.IsValueType) il.Emit(OpCodes.Box, info.PropertyType);
            il.Emit(OpCodes.Ret);
            return (Func<T, object>)dynamicMethod.CreateDelegate(typeof(Func<T, object>));
        }

        private static Action<T, object> CreateSetterDelegate(PropertyInfo info)
        {
            var dynamicMethod = new DynamicMethod(Guid.NewGuid().ToString("N") + "_SetterDelegate", typeof(void), new[] { typeof(T), typeof(object) });
            var il = dynamicMethod.GetILGenerator();
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldarg_1);
            if (info.PropertyType.IsValueType)
                il.Emit(OpCodes.Unbox_Any, info.PropertyType);
            else
                il.Emit(OpCodes.Castclass, info.PropertyType);
            il.Emit(OpCodes.Callvirt, info.GetSetMethod());
            il.Emit(OpCodes.Ret);
            return (Action<T, object>)dynamicMethod.CreateDelegate(typeof(Action<T, object>));
        }
    }

    public class SqlRepo<T> : IBaseRepo<T> where T : class, new()
    {
        public ConnectionStringSettings ConnectionConfig { get; private set; }
        public DbProviderFactory ProviderFactory { get; private set; }
        private DbConnection connection;
        public DbConnection Connection
        {
            get
            {
                if (connection == null)
                {
                    connection = ProviderFactory.CreateConnection();
                    connection.ConnectionString = ConnectionConfig.ConnectionString;
                }
                return connection;
            }
        }
        private SqlQueryProvider queryProvider;
        public SqlQueryProvider QueryProvider
        {
            get
            {
                if (queryProvider == null)
                {
                    queryProvider = new SqlQueryProvider(Connection);
                    if (EntityContext<T>.Type.Name != EntityContext<T>.TableName)
                        queryProvider.TableNames[EntityContext<T>.Type] = EntityContext<T>.TableName;
                }
                return queryProvider;
            }

        }

        public SqlRepo(string name, string connectionString, string providerName)
        {
            ConnectionConfig = new ConnectionStringSettings(name, connectionString, providerName);
            ProviderFactory = DbProviderFactories.GetFactory(ConnectionConfig.ProviderName);
            if (ProviderFactory == null)
                throw new Exception(ConnectionConfig.ProviderName + " not find");
        }

        public SqlRepo(ConnectionStringSettings connectionConfig)
        {
            ConnectionConfig = connectionConfig;
            ProviderFactory = DbProviderFactories.GetFactory(ConnectionConfig.ProviderName);
            if (ProviderFactory == null)
                throw new Exception(ConnectionConfig.ProviderName + " not find");
        }

        public SqlRepo(string connectionName = "DefaultConnection")
        {
            if (connectionName == null)
                throw new ArgumentException("connectionName is null");
            ConnectionConfig = ConfigurationManager.ConnectionStrings[connectionName];
            if (ConnectionConfig == null)
                throw new ArgumentException("ConnectionConfig is null");
            ProviderFactory = DbProviderFactories.GetFactory(ConnectionConfig.ProviderName);
            if (ProviderFactory == null)
                throw new Exception(ConnectionConfig.ProviderName + " not find");
        }

        public IQueryable<T> Query
        {
            get
            {
                return QueryProvider.CreateQuery<T>();
            }
        }

        public void Add(T entity)
        {
            ColumnInfo<T> autoKeyColumns = null;
            var key = new StringBuilder();
            var value = new StringBuilder();
            var dbParameterList = new DynamicParameters(entity);
            foreach (var currColumn in EntityContext<T>.Columns)
            {
                if (currColumn.Key && currColumn.Autoincrement)
                {
                    autoKeyColumns = currColumn;
                    continue;
                }
                key.Append(string.Format("{0},", currColumn.ColumnName));
                value.Append(string.Format("@{0},", currColumn.ColumnName));
            }
            if (key.Length > 0) key.Remove(key.Length - 1, 1);
            if (value.Length > 0) value.Remove(value.Length - 1, 1);
            var sql = string.Format("insert into {0} ({1}) values ({2});", EntityContext<T>.TableName, key.ToString(), value.ToString());
            OpenConnection();
            if (autoKeyColumns != null)
            {
                sql += GetLastIdSql();
                var result = connection.QueryFirst(sql, dbParameterList);
                if (result != null)
                    autoKeyColumns.SetterDelegate(entity, Convert.ToInt32(result.id));
            }
            else
                connection.Execute(sql, dbParameterList);
        }
        private void OpenConnection()
        {
            if (Connection.State != ConnectionState.Open)
                Connection.Open();
        }
        private string GetLastIdSql()
        {
            if (ConnectionConfig.ProviderName == "System.Data.SqlClient")
                return "SELECT @@IDENTITY  as id;";
            else
                return "SELECT LAST_INSERT_ID() as id;";
        }

        public void Dispose()
        {
            if (connection != null && connection.State != ConnectionState.Closed)
                connection.Close();
        }

        public T Get(object key)
        {
            if (key is int && (int)key == 0)
                return null;
            var param = new DynamicParameters();
            param.Add("@p0", key);
            if (EntityContext<T>.Key == null)
                throw new Exception("实体模型未设置主键");
            var sql = string.Format("select * from {0} where {1} = @p0", EntityContext<T>.TableName, EntityContext<T>.Key.ColumnName);
            OpenConnection();
            return Connection.QueryFirstOrDefault<T>(sql, param);
        }

        public void Remove(T entity)
        {
            var param = new DynamicParameters();
            param.Add("@p0", EntityContext<T>.Key.GetterDelegate(entity));
            if (EntityContext<T>.Key == null)
                throw new Exception("实体模型未设置主键");
            var sql = string.Format("delete from {0} where {1} = @p0", EntityContext<T>.TableName, EntityContext<T>.Key.ColumnName);
            OpenConnection();
            Connection.Execute(sql, param);
        }

        public void SaveChanges()
        {
        }

        public void Update(T entity)
        {
            if (EntityContext<T>.Key == null)
                throw new Exception("实体模型未设置主键");
            var where = string.Format("{0}=@{0}", EntityContext<T>.Key.ColumnName);
            var valueStr = new StringBuilder();
            var columnCount = EntityContext<T>.Columns.Count();
            var dbParameterList = new DynamicParameters(entity);

            foreach (var currColumn in EntityContext<T>.Columns)
            {
                if (currColumn.Key)
                    continue;
                valueStr.Append(string.Format("{0}=@{0},", currColumn.ColumnName));
            }
            if (valueStr.Length > 0) valueStr.Remove(valueStr.Length - 1, 1);
            var sql = string.Format("update {0} set {1} where {2}", EntityContext<T>.TableName, valueStr.ToString(), where);
            OpenConnection();
            Connection.Execute(sql, dbParameterList);
        }

        public void BatchUpdate(Expression<Func<T, bool>> expression, object data)
        {
            OpenConnection();
            var converter = new SqlConverter(typeof(T), Connection.GetType());
            var where = converter.ExpressionToWhere(expression);
            var param = converter.DynamicParameters;
            var dictionary = ObjectUtility.ObjectToDictionary(data);

            var valueStr = new StringBuilder();
            var columnCount = EntityContext<T>.Columns.Count();
            foreach (var currColumn in dictionary)
            {
                valueStr.Append(string.Format("{0}=@{0},", currColumn.Key));
                param.Add(currColumn.Key, currColumn.Value);
            }
            if (valueStr.Length > 0) valueStr.Remove(valueStr.Length - 1, 1);
            var sql = string.Format("update {0} set {1} where {2}", EntityContext<T>.TableName, valueStr.ToString(), where);
            Connection.Execute(sql, param);
        }

        public void BatchDelete(Expression<Func<T, bool>> expression)
        {
            OpenConnection();
            var converter = new SqlConverter(typeof(T), Connection.GetType());
            var where = converter.ExpressionToWhere(expression);
            var param = converter.DynamicParameters;
            var sql = string.Format("delete from {0}{1}", EntityContext<T>.TableName, where);
            Connection.Execute(sql, param);
        }
    }
}
