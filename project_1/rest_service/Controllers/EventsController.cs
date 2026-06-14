using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using rest_service.Models;

/// <summary>
/// Kontroler za upravljanje događajima u IoT sistemu kontrole pristupa.
/// Omogućava pregled, filtriranje, agregaciju i kreiranje događaja.
/// </summary>
[ApiController]
[Route("api/[controller]")]
public class EventsController : ControllerBase
{
    private readonly AccessControlSystemContext _context;

    public EventsController(AccessControlSystemContext context)
    {
        _context = context;
    }

    /// <summary>
    /// Vraća listu događaja sa podrškom za paginaciju.
    /// </summary>
    /// <param name="page">Broj stranice (podrazumevano 1).</param>
    /// <param name="pageSize">Broj rezultata po stranici (1-100).</param>
    /// <returns>Lista događaja sortirana po vremenu nastanka opadajuće.</returns>
    [HttpGet]
    [ProducesResponseType(typeof(IEnumerable<Event>), StatusCodes.Status200OK)]
    public async Task<ActionResult<IEnumerable<Event>>> GetEvents(
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50)
    {
        page = Math.Max(page, 1);
        pageSize = Math.Clamp(pageSize, 1, 100);

        return await _context.Events
            .AsNoTracking()
            .OrderByDescending(e => e.Timestamp)
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .ToListAsync();
    }

    /// <summary>
    /// Vraća poslednjih 100 događaja filtriranih po identifikatoru uređaja.
    /// </summary>
    /// <param name="deviceId">Identifikator uređaja.</param>
    /// <returns>Filtrirana lista događaja.</returns>
    [HttpGet("filter")]
    [ProducesResponseType(typeof(IEnumerable<Event>), StatusCodes.Status200OK)]
    public async Task<ActionResult<IEnumerable<Event>>> GetFilteredEvents(
        [FromQuery] string? deviceId)
    {
        IQueryable<Event> query = _context.Events.AsNoTracking();

        if (!string.IsNullOrEmpty(deviceId))
        {
            query = query.Where(e => e.DeviceId == deviceId);
        }

        return await query
            .OrderByDescending(e => e.Timestamp)
            .Take(100)
            .ToListAsync();
    }

    /// <summary>
    /// Vraća selektivne podatke o događajima (DeviceId i CardUid)
    /// radi optimizacije prenosa podataka.
    /// </summary>
    /// <param name="page">Broj stranice.</param>
    /// <param name="pageSize">Broj rezultata po stranici.</param>
    /// <returns>Lista selektivnih DTO objekata.</returns>
    [HttpGet("selective")]
    [ProducesResponseType(typeof(IEnumerable<SelectiveEventDto>), StatusCodes.Status200OK)]
    public async Task<ActionResult<IEnumerable<SelectiveEventDto>>> GetSelectiveEvents(
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 50)
    {
        page = Math.Max(page, 1);
        pageSize = Math.Clamp(pageSize, 1, 100);

        return await _context.Events
            .AsNoTracking()
            .OrderByDescending(e => e.Timestamp)
            .Skip((page - 1) * pageSize)
            .Take(pageSize)
            .Select(e => new SelectiveEventDto
            {
                DeviceId = e.DeviceId,
                CardUid = e.CardUid
            })
            .ToListAsync();
    }

    /// <summary>
    /// Izvršava složen agregacioni upit nad događajima.
    /// Omogućava filtriranje po uređaju, kartici, tipu događaja,
    /// vremenskom opsegu i tekstualnoj pretrazi.
    /// </summary>
    /// <param name="queryParams">Parametri za filtriranje i agregaciju.</param>
    /// <returns>Agregirani statistički podaci o događajima.</returns>
    [HttpGet("heavy")]
    [ProducesResponseType(typeof(IEnumerable<HeavyQueryResultDto>), StatusCodes.Status200OK)]
    public async Task<ActionResult<IEnumerable<HeavyQueryResultDto>>> GetHeavyQuery(
        [FromQuery] EventQueryParameters queryParams)
    {
        IQueryable<Event> query = _context.Events.AsNoTracking();

        if (!string.IsNullOrEmpty(queryParams.DeviceId))
        {
            query = query.Where(e => e.DeviceId == queryParams.DeviceId);
        }

        if (!string.IsNullOrEmpty(queryParams.CardUid))
        {
            query = query.Where(e => e.CardUid == queryParams.CardUid);
        }

        if (!string.IsNullOrEmpty(queryParams.EventType))
        {
            query = query.Where(e => e.EventType == queryParams.EventType);
        }

        if (queryParams.FromDate.HasValue)
        {
            query = query.Where(e => e.Timestamp >= queryParams.FromDate.Value);
        }

        if (queryParams.ToDate.HasValue)
        {
            query = query.Where(e => e.Timestamp <= queryParams.ToDate.Value);
        }

        if (!string.IsNullOrEmpty(queryParams.SearchTerm))
        {
            string search = $"%{queryParams.SearchTerm}%";
            query = query.Where(e =>
                EF.Functions.Like(e.Zone, search) ||
                EF.Functions.Like(e.DoorId, search));
        }

        return await query
            .GroupBy(e => new { e.DeviceId, e.EventType, e.Zone })
            .Select(g => new HeavyQueryResultDto
            {
                DeviceId = g.Key.DeviceId,
                EventType = g.Key.EventType,
                Zone = g.Key.Zone,
                Count = g.Count(),
                AverageResponseTimeMs = g.Average(e => e.ResponseTimeMs),
                AverageBatteryVoltage = g.Average(e => e.BatteryVoltage),
                MinimumTemperature = g.Min(e => e.Temperature),
                MaximumTemperature = g.Max(e => e.Temperature),
                FirstTimestamp = g.Min(e => e.Timestamp),
                LastTimestamp = g.Max(e => e.Timestamp)
            })
            .OrderByDescending(e => e.Count)
            .Take(queryParams.PageSize)
            .ToListAsync();
    }

    /// <summary>
    /// Kreira novi događaj u sistemu.
    /// Ukoliko nisu prosleđeni EventId ili Timestamp,
    /// generišu se automatski.
    /// </summary>
    /// <param name="newEvent">Objekat događaja koji se kreira.</param>
    /// <returns>Kreirani događaj.</returns>
    [HttpPost]
    [ProducesResponseType(typeof(Event), StatusCodes.Status201Created)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<ActionResult<Event>> Create([FromBody] Event newEvent)
    {
        if (newEvent.EventId == Guid.Empty)
        {
            newEvent.EventId = Guid.NewGuid();
        }

        if (newEvent.Timestamp == default)
        {
            newEvent.Timestamp = DateTime.UtcNow;
        }

        _context.Events.Add(newEvent);
        await _context.SaveChangesAsync();

        return CreatedAtAction(nameof(GetEvents), new { id = newEvent.Id }, newEvent);
    }
}

/// <summary>
/// DTO koji sadrži samo osnovne informacije o događaju.
/// </summary>
public class SelectiveEventDto
{
    /// <summary>
    /// Identifikator uređaja.
    /// </summary>
    public string? DeviceId { get; set; }

    /// <summary>
    /// Jedinstveni identifikator RFID kartice.
    /// </summary>
    public string? CardUid { get; set; }
}

/// <summary>
/// Parametri za filtriranje i pretragu događaja.
/// </summary>
public class EventQueryParameters
{
    /// <summary>
    /// Identifikator uređaja.
    /// </summary>
    public string? DeviceId { get; set; }

    /// <summary>
    /// Jedinstveni identifikator RFID kartice.
    /// </summary>
    public string? CardUid { get; set; }

    /// <summary>
    /// Tip događaja.
    /// </summary>
    public string? EventType { get; set; }

    /// <summary>
    /// Početni datum vremenskog opsega.
    /// </summary>
    public DateTime? FromDate { get; set; }

    /// <summary>
    /// Krajnji datum vremenskog opsega.
    /// </summary>
    public DateTime? ToDate { get; set; }

    /// <summary>
    /// Tekst za pretragu zone ili vrata.
    /// </summary>
    public string? SearchTerm { get; set; }

    private int _pageSize = 50;

    /// <summary>
    /// Broj stranice rezultata.
    /// </summary>
    public int Page { get; set; } = 1;

    /// <summary>
    /// Maksimalan broj rezultata za vraćanje (1-100).
    /// </summary>
    public int PageSize
    {
        get => _pageSize;
        set => _pageSize = Math.Clamp(value, 1, 100);
    }
}

/// <summary>
/// DTO koji predstavlja agregirane statističke rezultate
/// složenog upita nad događajima.
/// </summary>
public class HeavyQueryResultDto
{
    /// <summary>
    /// Identifikator uređaja.
    /// </summary>
    public string? DeviceId { get; set; }

    /// <summary>
    /// Tip događaja.
    /// </summary>
    public string? EventType { get; set; }

    /// <summary>
    /// Zona u kojoj se događaji nalaze.
    /// </summary>
    public string? Zone { get; set; }

    /// <summary>
    /// Ukupan broj događaja u grupi.
    /// </summary>
    public int Count { get; set; }

    /// <summary>
    /// Prosečno vreme odziva u milisekundama.
    /// </summary>
    public double AverageResponseTimeMs { get; set; }

    /// <summary>
    /// Prosečan napon baterije uređaja.
    /// </summary>
    public double AverageBatteryVoltage { get; set; }

    /// <summary>
    /// Najniža izmerena temperatura.
    /// </summary>
    public float MinimumTemperature { get; set; }

    /// <summary>
    /// Najviša izmerena temperatura.
    /// </summary>
    public float MaximumTemperature { get; set; }

    /// <summary>
    /// Vreme prvog događaja u grupi.
    /// </summary>
    public DateTime FirstTimestamp { get; set; }

    /// <summary>
    /// Vreme poslednjeg događaja u grupi.
    /// </summary>
    public DateTime LastTimestamp { get; set; }
}