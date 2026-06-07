using Microsoft.EntityFrameworkCore;
using Npgsql.EntityFrameworkCore.PostgreSQL;
using grpc_service.Models;
using grpc_service.Services;
using grpc_service.Data;

var builder = WebApplication.CreateBuilder(args);

// 1. Registruj EF DbContext
// 1. Registruj EF DbContext
builder.Services.AddDbContext<AccessControlDbContext>(options =>
{
    options.UseNpgsql(builder.Configuration.GetConnectionString("DefaultConnection"));
});
// 2. Dodaj gRPC i obavezno Reflection SERVIS u DI kontejner
builder.Services.AddGrpc(options =>
{
    options.MaxReceiveMessageSize = 100 * 1024 * 1024; // 100 MB
    options.MaxSendMessageSize = 100 * 1024 * 1024;    // 100 MB
});
builder.Services.AddGrpcReflection(); // <-- PROVERI OVO

builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(8080, o =>
    {
        Console.WriteLine("Kestrel listening on 8080 with HTTP/2");
        o.Protocols = Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols.Http2;
    });
});
var app = builder.Build();

// 3. Mapiraj tvoj servis
app.MapGrpcService<AccessControlGrpcService>();

// 4. MAPIRAJ Reflection servis (da bi Postman mogao da čita)

app.MapGrpcReflectionService(); // <-- PROVERI OVO
// Ako ne radi u Dockeru, skloni 'if' i ostavi samo:
// app.MapGrpcReflectionService();

app.Run();