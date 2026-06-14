using grpc_service.Data;
using grpc_service.Services;
using Microsoft.EntityFrameworkCore;

AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior", true);

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<AccessControlDbContext>(options =>
{
    options.UseNpgsql(builder.Configuration.GetConnectionString("DefaultConnection"));
});

builder.Services.AddGrpc(options =>
{
    options.MaxReceiveMessageSize = 100 * 1024 * 1024;
    options.MaxSendMessageSize = 100 * 1024 * 1024;
});
builder.Services.AddGrpcReflection();

builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(8080, o =>
    {
        o.Protocols = Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols.Http2;
    });
});

var app = builder.Build();

app.MapGrpcService<AccessControlGrpcService>();
app.MapGrpcReflectionService();

app.Run();
