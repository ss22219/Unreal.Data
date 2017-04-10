using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Unreal.Data.Interface
{
    public interface IBaseRepo : IDisposable
    {
        void SaveChanges();
    }

    public interface IBaseRepo<T> : IBaseRepo where T : class, new()
    {
        void Add(T entity);
        void Remove(T entity);
        void Update(T entity);
        T Get(object key);
        IQueryable<T> Query { get; }
    }
}
