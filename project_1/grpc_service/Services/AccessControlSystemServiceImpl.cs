using accesscontrol;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using grpc_service.Data;
using Microsoft.EntityFrameworkCore;

using DbEvent = grpc_service.Models.Event;
using GrpcEvent = accesscontrol.Event;

namespace grpc_service.Services;

public class AccessControlGrpcService : AccessControlService.AccessControlServiceBase
{
    private readonly AccessControlDbContext _context;

    public AccessControlGrpcService(AccessControlDbContext context)
    {
        _context = context;
    }

    public override async Task<GetEventResponse> GetEvent(GetEventRequest request, ServerCallContext context)
    {
        var ev = await _context.events
            .AsNoTracking()
            .FirstOrDefaultAsync(x => x.Id == request.Id);

        if (ev == null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, "Event not found"));
        }

        return new GetEventResponse
        {
            Event = MapToProto(ev)
        };
    }

    public override async Task<GetEventsResponse> GetEvents(GetEventsRequest request, ServerCallContext context)
    {
        var page = Math.Max(request.Page, 1);
        var pageSize = Math.Clamp(request.PageSize == 0 ? 50 : request.PageSize, 1, 100);

        var dbEvents = await _context.events
            .AsNoTracking()
            .OrderByDescending(x => x.Timestamp)
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .ToListAsync();

        var response = new GetEventsResponse();
        response.Events.AddRange(dbEvents.Select(MapToProto));
        return response;
    }

    public override async Task<SelectiveEventsResponse> GetSelectiveEvents(SelectiveEventsRequest request, ServerCallContext context)
    {
        var page = Math.Max(request.Page, 1);
        var pageSize = Math.Clamp(request.PageSize == 0 ? 50 : request.PageSize, 1, 100);

        var events = await _context.events
            .AsNoTracking()
            .OrderByDescending(x => x.Timestamp)
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .Select(e => new SelectiveEvent
            {
                DeviceId = e.DeviceId,
                CardUid = e.CardUid
            })
            .ToListAsync();

        var response = new SelectiveEventsResponse();
        response.Events.AddRange(events);
        return response;
    }

    public override async Task<HeavyQueryResponse> GetHeavyQuery(HeavyQueryRequest request, ServerCallContext context)
    {
        IQueryable<DbEvent> query = _context.events.AsNoTracking();

        if (!string.IsNullOrEmpty(request.DeviceId))
        {
            query = query.Where(e => e.DeviceId == request.DeviceId);
        }

        if (!string.IsNullOrEmpty(request.CardUid))
        {
            query = query.Where(e => e.CardUid == request.CardUid);
        }

        if (!string.IsNullOrEmpty(request.EventType))
        {
            query = query.Where(e => e.EventType == request.EventType);
        }

        if (request.FromDate != null)
        {
            query = query.Where(e => e.Timestamp >= request.FromDate.ToDateTime());
        }

        if (request.ToDate != null)
        {
            query = query.Where(e => e.Timestamp <= request.ToDate.ToDateTime());
        }

        if (!string.IsNullOrEmpty(request.SearchTerm))
        {
            var search = $"%{request.SearchTerm}%";
            query = query.Where(e => EF.Functions.Like(e.Zone, search) || EF.Functions.Like(e.DoorId, search));
        }

        var pageSize = Math.Clamp(request.PageSize == 0 ? 50 : request.PageSize, 1, 100);

        var results = await query
            .GroupBy(e => new { e.DeviceId, e.EventType, e.Zone })
            .Select(g => new
            {
                g.Key.DeviceId,
                g.Key.EventType,
                g.Key.Zone,
                Count = g.Count(),
                AverageResponseTimeMs = g.Average(e => e.ResponseTimeMs),
                AverageBatteryVoltage = g.Average(e => e.BatteryVoltage),
                MinimumTemperature = g.Min(e => e.Temperature),
                MaximumTemperature = g.Max(e => e.Temperature),
                FirstTimestamp = g.Min(e => e.Timestamp),
                LastTimestamp = g.Max(e => e.Timestamp)
            })
            .OrderByDescending(e => e.Count)
            .Take(pageSize)
            .ToListAsync();

        var response = new HeavyQueryResponse();
        response.Results.AddRange(results.Select(r => new HeavyQueryResult
        {
            DeviceId = r.DeviceId,
            EventType = r.EventType,
            Zone = r.Zone,
            Count = r.Count,
            AverageResponseTimeMs = r.AverageResponseTimeMs,
            AverageBatteryVoltage = r.AverageBatteryVoltage,
            MinimumTemperature = r.MinimumTemperature,
            MaximumTemperature = r.MaximumTemperature,
            FirstTimestamp = ToTimestamp(r.FirstTimestamp),
            LastTimestamp = ToTimestamp(r.LastTimestamp)
        }));

        return response;
    }

    public override async Task<CreateEventResponse> CreateEvent(CreateEventRequest request, ServerCallContext context)
    {
        var ev = MapToDb(request.Event);
        _context.events.Add(ev);
        await _context.SaveChangesAsync();

        return new CreateEventResponse
        {
            Event = MapToProto(ev)
        };
    }

    private static GrpcEvent MapToProto(DbEvent ev)
    {
        return new GrpcEvent
        {
            Id = ev.Id,
            EventId = ev.EventId.ToString(),
            Timestamp = ToTimestamp(ev.Timestamp),
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

    private static DbEvent MapToDb(GrpcEvent ev)
    {
        var timestamp = ev.Timestamp?.ToDateTime() ?? DateTime.UtcNow;

        return new DbEvent
        {
            EventId = Guid.TryParse(ev.EventId, out var eventId) ? eventId : Guid.NewGuid(),
            Timestamp = DateTime.SpecifyKind(timestamp, DateTimeKind.Unspecified),
            DeviceId = string.IsNullOrEmpty(ev.DeviceId) ? "RFID-GRPC" : ev.DeviceId,
            CardUid = string.IsNullOrEmpty(ev.CardUid) ? "00000000" : ev.CardUid,
            AccessGranted = ev.AccessGranted,
            DoorId = string.IsNullOrEmpty(ev.DoorId) ? "UNKNOWN" : ev.DoorId,
            Zone = string.IsNullOrEmpty(ev.Zone) ? "UNKNOWN" : ev.Zone,
            SignalStrength = ev.SignalStrength,
            BatteryVoltage = ev.BatteryVoltage,
            ResponseTimeMs = ev.ResponseTimeMs,
            EventType = string.IsNullOrEmpty(ev.EventType) ? "RFID_SCAN" : ev.EventType,
            Temperature = ev.Temperature
        };
    }

    private static Timestamp ToTimestamp(DateTime value)
    {
        return Timestamp.FromDateTime(DateTime.SpecifyKind(value, DateTimeKind.Utc));
    }
}
