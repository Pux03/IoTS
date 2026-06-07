using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using rest_service.Models; // Tvoj namespace

[ApiController]
[Route("api/[controller]")]
public class EventsController : ControllerBase
{
    private readonly AccessControlSystemContext _context;

    public EventsController(AccessControlSystemContext context)
    {
        _context = context;
    }

    [HttpGet("filter")]
    public async Task<ActionResult<IEnumerable<Event>>> GetFilteredEvents([FromQuery] string? deviceId)
    {
        // Ispravljeno: AsQueryable() omogućava dinamičko građenje upita
        IQueryable<Event> query = _context.Events;

        if (!string.IsNullOrEmpty(deviceId))
        {
            query = query.Where(e => e.DeviceId == deviceId);
        }

        // Uvek dodaj bar Take() da zaštitiš bazu od prevelikog broja rezultata
        return await query
            .OrderByDescending(e => e.Timestamp)
            .Take(100)
            .ToListAsync();
    }

    [HttpGet("selective")]
    public async Task<ActionResult<IEnumerable<SelectiveEventDto>>> GetSelectiveEvents([FromQuery] int page = 1, [FromQuery] int pageSize = 50)
    {
        return await _context.Events
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

    [HttpGet("heavy")]
    public async Task<ActionResult<IEnumerable<SelectiveEventDto>>> GetHeavyQuery([FromQuery] EventQueryParameters queryParams)
    {
        // 1. Započinjemo upit nad bazom
        IQueryable<Event> query = _context.Events;

        // 2. Filtriranje po tačnim vrednostima (Jednostavni filteri)
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

        // 3. Filtriranje po opsegu datuma (Veoma važno za logove/događaje!)
        if (queryParams.FromDate.HasValue)
        {
            query = query.Where(e => e.Timestamp >= queryParams.FromDate.Value);
        }

        if (queryParams.ToDate.HasValue)
        {
            query = query.Where(e => e.Timestamp <= queryParams.ToDate.Value);
        }

        // 4. Tekstualna pretraga (Slobodan unos - npr. traženje zone ili vrata)
        if (!string.IsNullOrEmpty(queryParams.SearchTerm))
        {
            // Koristimo EF.Functions.Like za bolje performanse na SQL nivou
            string search = $"%{queryParams.SearchTerm}%";
            query = query.Where(e => EF.Functions.Like(e.Zone, search) || EF.Functions.Like(e.DoorId, search));
        }

        // 5. Sortiranje, Paginacija i selekcija kolona (Izvršavanje upita)
        return await query
            .OrderByDescending(e => e.Timestamp)
            .Skip((queryParams.Page - 1) * queryParams.PageSize)
            .Take(queryParams.PageSize)
            .Select(e => new SelectiveEventDto
            {
                DeviceId = e.DeviceId,
                CardUid = e.CardUid
                // Ovde možeš dodati još par kolona ako ti zatrebaju za teži upit
            })
            .ToListAsync();
    }

    [HttpGet]
    public async Task<ActionResult<IEnumerable<Event>>> GetEvents([FromQuery] int page = 1, [FromQuery] int pageSize = 50)
    {

        return await _context.Events
            .OrderByDescending(e => e.Timestamp) // Najnoviji događaji prvi
            .Skip((page - 1) * pageSize)         // Preskoči prethodne stranice
            .Take(pageSize)                      // Uzmi samo koliko ti treba
            .ToListAsync();
    }


    [HttpPost]
    public async Task<ActionResult<Event>> Create([FromBody] Event newEvent)
    {
        _context.Events.Add(newEvent);
        await _context.SaveChangesAsync();
        return CreatedAtAction(nameof(GetEvents), new { id = newEvent.EventId }, newEvent);
    }
}

public class SelectiveEventDto
{
    public string? DeviceId { get; set; }
    public string? CardUid { get; set; }
}

public class EventQueryParameters
{
    public string? DeviceId { get; set; }
    public string? CardUid { get; set; }
    public string? EventType { get; set; }
    public DateTime? FromDate { get; set; }
    public DateTime? ToDate { get; set; }
    public string? SearchTerm { get; set; } // Za pretragu po zonama/vratima

    // Paginacija sa zaštitom (maksimalno 100 zapisa po stranici)
    private int _pageSize = 50;
    public int Page { get; set; } = 1;
    public int PageSize
    {
        get => _pageSize;
        set => _pageSize = (value > 100) ? 100 : value;
    }
}