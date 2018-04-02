let columnDefs = [
  { field: "athlete", filter: 'text', enableRowGroup: true, enablePivot: false },
  { field: "age", enableRowGroup: true, enablePivot: false },
  { field: "country", enableRowGroup: true, rowGroup: true, enablePivot: true, hide: true },
  { field: "year", filter: 'number', enableRowGroup: true, rowGroup: true, enablePivot: false, hide: true },
  {
    headerName: "Sport", field: "sport", filter: 'agSetColumnFilter',
    filterParams: {
      values: ["Speed Skating", "Cross Country Skiing", "Diving", "Biathlon", "Ski Jumping", "Nordic Combined", "Athletics", "Table Tennis", "Tennis", "Synchronized Swimming", "Rowing", "Equestrian", "Canoeing", "Badminton", "Weightlifting", "Waterpolo", "Triathlon", "Taekwondo", "Softball", "Snowboarding", "Sailing", "Modern Pentathlon", "Ice Hockey", "Hockey", "Football", "Freestyle Skiing", "Curling", "Beach Volleyball", "Swimming", "Gymnastics", "Short-Track Speed Skating", "Cycling", "Alpine Skiing", "Shooting", "Fencing", "Bobsleigh", "Archery", "Wrestling", "Volleyball", "Trampoline", "Skeleton", "Rhythmic Gymnastics", "Luge", "Judo", "Handball", "Figure Skating", "Baseball", "Boxing", "Basketball"],
      newRowsAction: 'keep'
    },
    enableRowGroup: true,
    enablePivot: true
  },
  { field: "gold", type: "measure" },
  { field: "silver", type: "measure" },
  { field: "bronze", type: "measure" },
  { field: "total", type: "measure" }
];

let gridOptions = {
  columnTypes: {
    measure: {
      width: 150,
      enableValue: true,
      aggFunc: 'sum',
      allowedAggFuncs: ['sum']
    }
  },

  defaultColDef: {
    width: 180,
    filter: "agNumberColumnFilter",
    filterParams: {
      applyButton: true,
      newRowsAction: 'keep'
    }
  },
  enableSorting: true,
  enableFilter: true,
  columnDefs: columnDefs,
  enableColResize: true,
  rowModelType: 'enterprise',
  // bring back data 50 rows at a time
  cacheBlockSize: 100,
  rowGroupPanelShow: 'always',
  pivotPanelShow: 'always'
};

function EnterpriseDatasource() {}

EnterpriseDatasource.prototype.getRows = function (params) {
  let request = params.request;

  let jsonRequest = JSON.stringify(request, null, 2);
  console.log(jsonRequest);

  let httpRequest = new XMLHttpRequest();
  httpRequest.open('POST', 'http://localhost:9090/getRows');
  httpRequest.setRequestHeader("Content-type", "application/json");
  httpRequest.send(jsonRequest);
  httpRequest.onreadystatechange = () => {
    if (httpRequest.readyState === 4 && httpRequest.status === 200) {
      let result = JSON.parse(httpRequest.responseText);
      params.successCallback(result.data, result.lastRow);

      updateSecondaryColumns(request, result);
    }
  };
};

// setup the grid after the page has finished loading
document.addEventListener('DOMContentLoaded', function () {
  let gridDiv = document.querySelector('#myGrid');
  new agGrid.Grid(gridDiv, gridOptions);
  gridOptions.api.setEnterpriseDatasource(new EnterpriseDatasource());
});

let updateSecondaryColumns = function (request, result) {
  if (request.pivotMode && request.pivotCols.length > 0) {
    let secondaryColDefs = createSecondaryColumns(result.secondaryColumns);
    gridOptions.columnApi.setSecondaryColumns(secondaryColDefs);
  } else {
    gridOptions.columnApi.setSecondaryColumns([]);
  }
};

let createSecondaryColumns = function (fields) {
  let secondaryCols = [];

  function addColDef(colId, parts, res, isGroup) {
    if (parts.length === 0) return [];

    let first = parts.shift();
    let existing = res.find((r) => r.groupId === first);

    if (existing) {
      existing['children'] = addColDef(colId, parts, existing.children);
    } else {
      let colDef = {};
      if(isGroup) {
        colDef['groupId'] = first;
        colDef['headerName'] = first;
      } else {
        colDef['colId'] = colId;
        colDef['headerName'] = "sum(" + first + ")";
        colDef['field'] = colId;
      }

      let children = addColDef(colId, parts, []);
      children.length > 0 ? colDef['children'] = children : null;

      res.push(colDef);
    }

    return res;
  }

  fields.forEach(field => addColDef(field, field.split('_'), secondaryCols, true));
  return secondaryCols;
};