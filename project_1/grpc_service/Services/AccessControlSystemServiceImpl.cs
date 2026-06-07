using Grpc.Core;
using accesscontrol;
using grpc_service.Data;
using grpc_service.Models;
using Google.Protobuf.WellKnownTypes;
using Microsoft.EntityFrameworkCore;

using DbEvent = grpc_service.Models.Event;
using GrpcEvent = accesscontrol.Event;

namespace grpc_service.Services;

public class AccessControlGrpcService
    : AccessControlService.AccessControlServiceBase
{
    private readonly AccessControlDbContext _context;

    public AccessControlGrpcService(AccessControlDbContext context)
    {
        _context = context;
    }

    public override async Task<GetEventResponse> GetEvent(
        GetEventRequest request,
        ServerCallContext context)
    {
        var ev = await _context.events
            .FirstOrDefaultAsync(x => x.Id == request.Id);

        if (ev == null)
            throw new RpcException(new Status(StatusCode.NotFound, "Event not found"));

        return new GetEventResponse
        {
            Event = MapToProto(ev)
        };
    }

    private GrpcEvent MapToProto(DbEvent ev)
    {
        return new GrpcEvent
        {
            Id = ev.Id,
            EventId = ev.EventId.ToString(),
            Timestamp = Timestamp.FromDateTime(ev.Timestamp.ToUniversalTime()),
            DeviceId = ev.DeviceId,
            CardUid = ev.CardUid,
            AccessGranted = ev.AccessGranted,
            DoorId = ev.DoorId,
            Zone = ev.Zone,
            SignalStrength = ev.SignalStrength,
            BatteryVoltage = ev.BatteryVoltage,
            ResponseTimeMs = ev.ResponseTimeMs,
            EventType = ev.EventType,
            Temperature = ev.Temperature
        };
    }

    public override async Task<GetEventsResponse> GetEvents(
    GetEventsRequest request,
    ServerCallContext context)
    {
        var dbEvents = await _context.events
            .OrderByDescending(x => x.Timestamp)
            .AsNoTracking()
            .ToListAsync();

        var response = new GetEventsResponse();
        foreach (var ev in dbEvents)
        {
            response.Events.Add(MapToProto(ev));
        }

        return response;
    }
}

