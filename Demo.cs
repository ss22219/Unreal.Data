using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Unreal.Data.SQL;

namespace Unreal.Data
{
    public class Model
    {
        [Key]
        public int id { get; set; }
        public string contents { get; set; }
    }

    public class Demo
    {
        public static void Main(string[] args)
        {
            var repo = new SqlRepo<Model>("test", "Data Source=123.207.111.61;User ID=sa;Password=Ss303384755;Initial Catalog=brnmall;Pooling=true", "System.Data.SqlClient");
            //var model1 = new Model() { contents = "1" };
            //var model2 = new Model() { contents = "2" };
            //repo.Add(model1);
            //repo.Add(model2);

            //var model3 = repo.Get(1);

            //model1.contents = "3";
            //repo.Update(model1);

            //repo.Remove(model2);
            //repo.SaveChanges();

            //var list = repo.Query.Where(m => m.id > 1).ToList();
            //var single = repo.Query.Where(m => m.id > 1).Single();
            //var subPage = repo.Query.Where(m => m.id > 1).OrderByDescending( m=> m.id ).Skip(1).Take(5).ToList();
            //var count = repo.Query.Where(m => m.id > 1).Count();
            var m1 = repo.Query.Select(m => new { id1 = m.id }).First();
        }
    }
}
