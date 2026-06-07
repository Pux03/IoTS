using Microsoft.EntityFrameworkCore;
using System.Text.RegularExpressions;
using grpc_service.Models;

namespace grpc_service.Data;

public class AccessControlDbContext : DbContext
{
    public AccessControlDbContext(DbContextOptions<AccessControlDbContext> options) : base(options) { }

    public DbSet<Event> events { get; set; } // ili kako god ti se zove DbSet

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        // Automatsko mapiranje svega u bazi na snake_case (mala slova s donjom crtom)
        foreach (var entity in modelBuilder.Model.GetEntityTypes())
        {
            var tableName = entity.GetTableName();
            if (tableName != null)
            {
                entity.SetTableName(ConvertToSnakeCase(tableName));
            }

            foreach (var property in entity.GetProperties())
            {
                property.SetColumnName(ConvertToSnakeCase(property.Name));
            }
        }
    }

    private string ConvertToSnakeCase(string input)
    {
        if (string.IsNullOrEmpty(input)) return input;
        return Regex.Replace(input, "([a-z0-9])([A-Z])", "$1_$2").ToLower();
    }
}