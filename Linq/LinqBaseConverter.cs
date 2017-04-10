using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Unreal.Data.Linq
{
    public class MemberInfoEx
    {
        private MemberInfo _memberInfo;

        public Type MemberType
        {
            get
            {
                return field != null ? field.FieldType : prop.PropertyType;
            }
        }

        public Type DeclaringType
        {
            get
            {
                return _memberInfo.DeclaringType;
            }
        }

        public string Name
        {
            get
            {
                return _memberInfo.Name;
            }
        }
        private System.Reflection.FieldInfo field;
        private System.Reflection.PropertyInfo prop;

        public void SetValue(object obj, object val)
        {
            if (field != null)
                field.SetValue(obj, val);
            else if (prop != null)
                prop.SetValue(obj, val);
        }
        public object GetValue(object obj)
        {
            return field != null ? field.GetValue(obj) : prop.GetValue(obj);
        }
        public MemberInfoEx(MemberInfo info)
        {
            _memberInfo = info;
            if (_memberInfo is FieldInfo)
                field = (FieldInfo)_memberInfo;
            else if (_memberInfo is PropertyInfo)
                prop = (PropertyInfo)_memberInfo;
        }
    }
    public class ConstSet
    {
        public Func<string> GetParamChar = () => "?";
        public string Gt = " > ";
        public string Lt = " < ";
        public string GtEq = " >= ";
        public string LtEq = " <= ";
        public string Eq = " == ";
        public string NullChar = "<NULL>";
        public string And = " && ";
        public string Or = " || ";
        public object True = true;
        public object False = false;
        public string OnlyTrue = "";
        public string OnlyFalse = "1 != 1";
        public string ParamChar { get { return GetParamChar(); } }
        public string NotEq = " != ";
    }
    public enum SelectMode
    {
        All,
        Count,
        Any,
        Select
    }
    public interface IMethodCallConverter
    {
        void Convert(MethodCallExpression expression);
    }

    public class MethodCallConverter : IMethodCallConverter
    {
        public MethodCallConverter(LinqBaseConverter baseConverter = null)
        {
            BaseConverter = baseConverter;
        }
        public LinqBaseConverter BaseConverter { get; set; }
        public virtual void Convert(MethodCallExpression expression)
        {
            if (!MehodConverters.ContainsKey(expression.Method.Name))
                throw new NotSupportedException("不支持 Queryable." + expression.Method.Name + "() 方法的查询");
            MehodConverters[expression.Method.Name](expression);
        }
        public Dictionary<string, Action<MethodCallExpression>> MehodConverters = new Dictionary<string, Action<MethodCallExpression>>();
    }

    public abstract class LinqBaseConverter
    {
        public SelectMode SelectMode = SelectMode.All;
        public Expression SelectExpression;
        public List<object> Params;
        protected int? take;
        protected bool? desc;
        protected int? skip;
        protected string orderBy = null;
        protected ConstSet ConstSet = new ConstSet();
        protected Dictionary<Type, IMethodCallConverter> MethodCallConverters = new Dictionary<Type, IMethodCallConverter>();
        protected bool isOpt { get; set; }
        protected StringBuilder sb { get; set; }
        public Type ElementType { get; set; }
        public LinqBaseConverter(Type elementType)
        {
            isOpt = false;
            sb = new StringBuilder();
            this.ElementType = elementType;
            MethodCallConverters[typeof(Queryable)] = new MethodCallConverter()
            {
                MehodConverters = new Dictionary<string, Action<MethodCallExpression>>()
                {
                    { "Where", QueryableWhere},
                    { "Single", QueryableSingle},
                    { "First", QueryableSingle},
                    { "FirstOrDefault", QueryableSingle},
                    { "SingleOrDefault", QueryableSingle},
                    { "Take", QueryableTake},
                    { "Skip", QueryableSkip},
                    { "OrderBy", QueryableOrderBy},
                    { "OrderByDescending", QueryableOrderByDescending},
                    { "Any", QueryableAny},
                    { "Count", QueryableCount},
                    { "Select", QueryableSelect},
                }
            };
            MethodCallConverters[typeof(System.Linq.Enumerable)] = new MethodCallConverter()
            {
                MehodConverters = new Dictionary<string, Action<MethodCallExpression>>
                {
                    { "Contains", EnumerableContains}
                }
            };
        }

        protected virtual string CloumnName(string name)
        {
            return name;
        }

        protected virtual string LimitParse()
        {
            if (skip != null || take != null)
            {
                return " limit " + (skip == null ? 0 : skip) + "," + (take == null ? -1 : take);
            }
            return null;
        }

        protected virtual string OrderByParse()
        {
            if (desc != null && orderBy != null)
            {
                return " order by " + orderBy + (desc == true ? " desc" : null);
            }
            return null;
        }

        protected object GetMemberExpressionValue(MemberExpression expression)
        {
            var member = new MemberInfoEx(expression.Member);
            var exp = expression.Expression;
            if (exp is ConstantExpression)
                return member.GetValue(((ConstantExpression)exp).Value);
            else if (exp == null)
            {
                return member.GetValue(null);
            }
            else if (exp is MemberExpression)
            {
                return member.GetValue(GetMemberExpressionValue((MemberExpression)exp));
            }
            return null;
        }

        protected virtual string BoolParse(bool val)
        {

            if (!isOpt)
                return (bool)val ? ConstSet.OnlyTrue : ConstSet.OnlyFalse;
            else
            {
                Params.Add((bool)val ? ConstSet.True : ConstSet.False);
                return ConstSet.ParamChar;
            }
        }

        protected virtual void ExpressionParse(LambdaExpression expression)
        {
            ExpressionParse(expression.Body);
        }

        protected virtual void ExpressionParse(ConstantExpression expression)
        {
            var val = expression.Value;
            if (val is IQueryable)
                return;
            else if (val is bool)
            {
                sb.Append(BoolParse((bool)val));
            }
            else
            {
                if (val != null)
                {
                    Params.Add(val);
                    sb.Append(ConstSet.ParamChar);
                }
                else
                {
                    sb.Append(ConstSet.NullChar);
                }
            }
        }

        protected virtual void ExpressionParse(BinaryExpression expression)
        {
            var left = expression.Left;
            var right = expression.Right;
            string opt = null;
            switch (expression.NodeType)
            {
                case ExpressionType.AndAlso:
                    opt = ConstSet.And;
                    break;
                case ExpressionType.GreaterThanOrEqual:
                    opt = ConstSet.GtEq;
                    break;
                case ExpressionType.LessThanOrEqual:
                    opt = ConstSet.LtEq;
                    break;
                case ExpressionType.OrElse:
                    opt = ConstSet.Or;
                    break;
                case ExpressionType.And:
                    opt = ConstSet.And;
                    break;
                case ExpressionType.GreaterThan:
                    opt = ConstSet.Gt;
                    break;
                case ExpressionType.LessThan:
                    opt = ConstSet.Lt;
                    break;
                case ExpressionType.Equal:
                    opt = ConstSet.Eq;
                    break;
                case ExpressionType.NotEqual:
                    opt = ConstSet.NotEq;
                    break;
                default:
                    break;
            }
            if (isOpt)
            {
                sb.Append("(");
                ExpressionParse(left);
                sb.Append(opt);
                ExpressionParse(right);
                sb.Append(")");
            }
            else
            {
                isOpt = true;
                ExpressionParse(left);
                sb.Append(opt);
                ExpressionParse(right);
            }
        }

        protected virtual void ExpressionParse(MemberExpression expression)
        {
            var member = new MemberInfoEx(expression.Member);
            if (expression.Expression is ParameterExpression)
            {
                if (member.MemberType == typeof(bool) && !isOpt)
                {
                    Params.Add(ConstSet.True);
                    sb.Append(CloumnName(member.Name) + " " + ConstSet.Eq + " " + ConstSet.ParamChar);
                }
                else
                    sb.Append(CloumnName(member.Name));
            }
            else
            {
                var val = GetMemberExpressionValue((MemberExpression)expression);
                if (val is bool)
                    sb.Append(BoolParse((bool)val));
                else
                {
                    if (val != null)
                    {
                        Params.Add(val);
                        sb.Append(ConstSet.ParamChar);
                    }
                    else
                    {
                        sb.Append(ConstSet.NullChar);
                    }
                }
            }
        }

        protected virtual void ExpressionParse(UnaryExpression expression)
        {
            if (!(expression.Operand is MemberExpression))
            {
                ExpressionParse(expression.Operand);
                return;
            }
            var member = new MemberInfoEx(((MemberExpression)expression.Operand).Member);
            switch (expression.NodeType)
            {
                case ExpressionType.Not:
                    Params.Add(ConstSet.True);
                    sb.Append(CloumnName(member.Name) + " " + ConstSet.Eq + " " + ConstSet.ParamChar);
                    break;
                case ExpressionType.NotEqual:
                    Params.Add(ConstSet.False);
                    sb.Append(CloumnName(member.Name) + " " + ConstSet.Eq + " " + ConstSet.ParamChar);
                    break;
                case ExpressionType.Convert:
                    Params.Add(member.GetValue(null));
                    sb.Append(ConstSet.ParamChar);
                    break;
                default:
                    sb.Append(CloumnName(member.Name));
                    break;
            }
        }
        protected virtual void ExpressionParse(MethodCallExpression expression)
        {
            var method = expression.Method;
            if (!MethodCallConverters.ContainsKey(method.DeclaringType))
                throw new NotSupportedException("不支持 " + method.DeclaringType.FullName + " 类型的方法调用");
            MethodCallConverters[method.DeclaringType].Convert(expression);
        }

        protected virtual void QueryableRight(MethodCallExpression expression)
        {
            int olen = sb.Length;
            ExpressionParse(expression.Arguments[0]);
            if (sb.Length != olen)
                isOpt = true;
        }

        protected virtual void QueryableWhere(MethodCallExpression expression)
        {
            QueryableRight(expression);
            if (expression.Arguments.Count == 2)
            {
                if (!isOpt)
                    ExpressionParse(expression.Arguments[1]);
                else
                {
                    sb.Append(ConstSet.And + "(");
                    ExpressionParse(expression.Arguments[1]);
                    sb.Append(")");
                }
                isOpt = true;
            }
        }

        protected virtual void QueryableSingle(MethodCallExpression expression)
        {
            take = 1;
            QueryableWhere(expression);
        }

        protected virtual void QueryableTake(MethodCallExpression expression)
        {
            take = (int)(((ConstantExpression)expression.Arguments[1]).Value);
            QueryableRight(expression);
        }

        protected virtual void QueryableSkip(MethodCallExpression expression)
        {
            skip = (int)(((ConstantExpression)expression.Arguments[1]).Value);
            QueryableRight(expression);
        }

        protected virtual void QueryableOrderBy(MethodCallExpression expression)
        {
            desc = false;
            orderBy = CloumnName(Member(expression.Arguments[1]).Name);
            QueryableRight(expression);
        }
        protected virtual void QueryableOrderByDescending(MethodCallExpression expression)
        {
            desc = true;
            orderBy = CloumnName(Member(expression.Arguments[1]).Name);
            QueryableRight(expression);
        }

        protected virtual void QueryableAny(MethodCallExpression expression)
        {
            SelectMode = SelectMode.Any;
            take = 1;
            QueryableWhere(expression);
        }

        protected virtual void QueryableCount(MethodCallExpression expression)
        {
            SelectMode = SelectMode.Count;
            QueryableWhere(expression);
        }

        protected virtual void QueryableSelect(MethodCallExpression expression)
        {
            SelectMode = SelectMode.Select;
            if (expression.Arguments[0] is MethodCallExpression)
                QueryableWhere((MethodCallExpression)expression.Arguments[0]);
            SelectExpression = expression.Arguments[1];
        }

        protected virtual string SelectExpressionParse()
        {
            if (SelectExpression == null)
                return null;
            if ((SelectExpression is UnaryExpression))
            {
                if (((UnaryExpression)SelectExpression).Operand is LambdaExpression)
                {
                    var body = ((LambdaExpression)(((UnaryExpression)SelectExpression).Operand)).Body;
                    if (body is MemberExpression)
                        return Member(body).Name;
                    else if (body is ParameterExpression)
                        return "*";
                    else if (body is NewExpression)
                    {
                        string argStr = "";
                        var args = ((NewExpression)body).Arguments;
                        if (args.Count == 0)
                            return "0";
                        for (int i = 0; i < args.Count; i++)
                        {
                            argStr += CloumnName(Member(args[i]).Name) + ",";
                        }
                        return argStr.TrimEnd(',');
                    }
                }
            }
            return null;
        }

        protected virtual void EnumerableContains(MethodCallExpression expression)
        {
            var method = expression.Method;
            var args = expression.Arguments;
            IEnumerable list = null;
            MemberInfo containsMember = null;
            MemberExpression listMemberExpression = null;
            containsMember = ((MemberExpression)args[1]).Member;
            listMemberExpression = (MemberExpression)args[0];
            list = (IEnumerable)GetMemberExpressionValue(listMemberExpression);
            sb.Append(InListParse(containsMember, list));
        }

        protected virtual string InListParse(MemberInfo containsMember, IEnumerable list)
        {
            return CloumnName(containsMember.Name) + " in ( " + ArrayParamParse(list) + " )";
        }

        protected virtual void ExpressionParse(Expression expression)
        {
            if (expression is LambdaExpression)
                ExpressionParse((LambdaExpression)expression);
            else if (expression is ConstantExpression)
                ExpressionParse((ConstantExpression)expression);
            else if (expression is BinaryExpression)
                ExpressionParse((BinaryExpression)expression);
            else if (expression is MemberExpression)
                ExpressionParse((MemberExpression)expression);
            else if (expression is UnaryExpression)
                ExpressionParse((UnaryExpression)expression);
            else if (expression is MethodCallExpression)
                ExpressionParse((MethodCallExpression)expression);
        }

        protected virtual string ArrayParamParse(IEnumerable obj)
        {
            if (obj == null)
                return null;
            var val = new StringBuilder();
            if (obj is System.Collections.IEnumerable)
            {
                var enumerator = ((IEnumerable)obj).GetEnumerator();
                while (enumerator.MoveNext())
                {
                    if (val.Length != 0)
                        val.Append(',');
                    val.Append(ConstSet.ParamChar);
                    Params.Add(enumerator.Current);
                }
                return val.ToString();
            }
            return null;
        }

        protected MemberInfoEx Member(Expression expression)
        {
            if (expression is LambdaExpression)
            {
                return Member(((LambdaExpression)expression).Body);
            }
            if (expression is UnaryExpression)
            {
                var operand = ((UnaryExpression)expression).Operand;
                if (operand is MemberExpression)
                    return new MemberInfoEx(((MemberExpression)operand).Member);
                else
                    return Member(operand);
            }
            else if (expression is MemberExpression)
            {
                return new MemberInfoEx(((MemberExpression)expression).Member);
            }
            return null;
        }
    }
}
