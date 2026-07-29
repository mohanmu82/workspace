// Shared event-type metadata (label + color) used by events.html, calendar.html and index.html
// so the "By Date" calendar views can render events consistently with the Events admin page.
(function (global) {
  const TYPES = [
    { id: 'HOLIDAY',        label: 'Holiday',                  color: '#20c997' },
    { id: 'DR_TEST',        label: 'Disaster Recovery Test',   color: '#6f42c1' },
    { id: 'CHANGE_FREEZE',  label: 'Change Freeze',            color: '#dc3545' },
    { id: 'MAINTENANCE',    label: 'Maintenance',              color: '#fd7e14' },
    { id: 'OTHER',          label: 'Other',                    color: '#0dcaf0' },
  ];
  const byId = {};
  TYPES.forEach(t => byId[t.id] = t);

  global.EventTypes = {
    all() { return TYPES; },
    label(id) { return (byId[id] || byId.OTHER).label; },
    color(id) { return (byId[id] || byId.OTHER).color; },
  };
})(window);
