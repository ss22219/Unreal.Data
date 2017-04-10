using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Dapper;
using System.Data.SqlClient;
using System.Text.RegularExpressions;

namespace Unreal.Data.Linq
{
    public class SqlConverter : LinqBaseConverter
    {
        protected override string LimitParse()
        {
            if (ConnectionType == typeof(SqlConnection))
                return (skip == null ? string.Empty : "\r\nOFFSET " + skip + " ROWS") + (take == null ? null : "\r\nFETCH NEXT " + take + " ROWS ONLY;");
            return base.LimitParse();
        }

        protected override string CloumnName(string name)
        {
            if (ConnectionType == typeof(SqlConnection))
                return string.Format("[{0}]", name);
            else
                return string.Format("`{0}`", name);
        }
        public Type ConnectionType { get; set; }

        public DynamicParameters DynamicParameters
        {
            get
            {
                if (Params == null || Params.Count == 0)
                    return null;
                DynamicParameters param = new DynamicParameters();
                for (int i = 0; i < Params.Count; i++)
                    param.Add("@p" + i, Params[i]);
                return param;
            }
        }

        private int paramCount = 0;
        public SqlConverter(Type elementType, Type connectionType) : base(elementType)
        {
            ConnectionType = connectionType;
            ConstSet.Eq = " = ";
            ConstSet.And = " and ";
            ConstSet.Or = " or ";
            ConstSet.NotEq = " <> ";
            ConstSet.True = "1";
            ConstSet.False = "0";
            ConstSet.OnlyTrue = "1=1";
            ConstSet.GetParamChar = () => "@p" + paramCount++;
        }

        protected virtual string SelectParse()
        {
            switch (SelectMode)
            {
                case SelectMode.Count:
                    return "count(*)";
                case SelectMode.Any:
                    return "1 as any";
                case SelectMode.Select:
                    return SelectExpressionParse();
                default:
                    break;
            }
            return "*";
        }

        public virtual string ExpressionConvert(Expression expression)
        {
            Params = new List<object>();
            ExpressionParse(expression);
            var where = WhereParse();
            var sql = string.Format("select {0} from {1}{2}{3}{4}", SelectParse(), CloumnName(ElementType.Name), string.IsNullOrEmpty(where) ? null : " where " + where, OrderByParse(), LimitParse());
            return sql;
        }

        private string WhereParse()
        {
            sb.Replace(ConstSet.Eq + ConstSet.NullChar, " IS NULL");
            var str = sb.ToString();
            if (str.IndexOf(ConstSet.NullChar) != -1)
            {
                str = new Regex(@"\(([^\(\)]+)" + ConstSet.NotEq + ConstSet.NullChar + @"\)").Replace(str, "!($1 IS NULL)");
            }
            return str;
        }
    }

    public class SqlQueryProvider : IQueryProvider
    {
        protected IDbConnection connection;
        Type connectionType;
        public SqlQueryProvider(IDbConnection dbConnection)
        {
            connection = dbConnection;
            connectionType = connection.GetType();
        }

        public IQueryable CreateQuery(Expression expression)
        {
            return null;
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression = null)
        {
            return new SqlQuery<TElement>(this, expression);
        }

        public object Execute(Expression expression)
        {
            return null;
        }

        public TResult Execute<TResult>(Expression expression)
        {
            Type elementType;
            if (!typeof(TResult).IsValueType)
                return Query<TResult>(expression).FirstOrDefault();
            else
            {
                elementType = GetElementType(expression);
                var result = Query(elementType, expression);
                return result == null ? default(TResult) : (TResult)result;
            }
        }

        private Type GetElementType(Expression expression)
        {
            if (expression is MethodCallExpression)
                return GetElementType(((MethodCallExpression)expression).Arguments[0]);
            else
                return ((IQueryable)((ConstantExpression)expression).Value).ElementType;
        }

        public object Query(Type elementType, Expression expression)
        {
            var converter = new SqlConverter(elementType, connectionType);
            var sql = converter.ExpressionConvert(expression);
            OpenConnecion();
            object result = null;
            switch (converter.SelectMode)
            {
                case SelectMode.Count:
                    result = connection.QueryFirstOrDefault<int>(sql, converter.DynamicParameters);
                    break;
                case SelectMode.Any:
                    result = connection.Query(sql, converter.DynamicParameters).Any();
                    break;
                case SelectMode.Select:
                case SelectMode.All:
                    result = connection.QueryFirstOrDefault(elementType, sql, converter.DynamicParameters);
                    break;
                default:
                    break;
            }
            return result;
        }

        public void OpenConnecion()
        {

            if (connection.State == ConnectionState.Closed)
                connection.Open();
        }

        public IEnumerable<T> Query<T>(Expression expression)
        {
            var elementType = GetElementType(expression);
            var converter = new SqlConverter(elementType, connectionType);
            var sql = converter.ExpressionConvert(expression);
            OpenConnecion();
            return connection.Query<T>(sql, converter.DynamicParameters);
        }
    }

    public class SqlQuery<T> : IQueryable<T>, IOrderedQueryable<T>
    {
        protected SqlQueryProvider provider;
        protected Expression expression;
        protected Type elementType;

        public SqlQuery(SqlQueryProvider provider, Expression expression = null)
        {
            this.elementType = typeof(T);
            this.provider = provider;
            if (expression == null)
                this.expression = Expression.Constant(this);
            else
                this.expression = expression;
        }


        public Type ElementType
        {
            get
            {
                return elementType;
            }
        }

        public IQueryProvider Provider
        {
            get
            {
                return this.provider;
            }
        }

        public Expression Expression
        {
            get
            {
                return expression;
            }
        }

        public IEnumerator GetEnumerator()
        {
            return null;
        }

        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            return provider.Query<T>(expression).GetEnumerator();
        }
    }
}
