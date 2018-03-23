let columnDefs = [
  {
    field: "athlete",
    filter: 'agSetColumnFilter',
    filterParams: {
      values: params => agGrid.simpleHttpRequest({
        url: 'http://localhost:9999/olympic-medals/getAthletes'
      }).then(data => params.success(data)),
      newRowsAction: 'keep'
    },
    enableRowGroup: true,
    enablePivot: true,
  },
  { field: "age", enableRowGroup: true, enablePivot: true },
  { field: "country", enableRowGroup: true, rowGroup: true, enablePivot: true, hide: true },
  {
    field: "year",
    filter: 'agSetColumnFilter',
    filterParams: {
      values: params => agGrid.simpleHttpRequest({url: 'http://localhost:9999/olympic-medals/getYears'})
        .then(data => params.success(data)),
      newRowsAction: 'keep'
    },
    enableRowGroup: true,
    enablePivot: true,
    // rowGroup: true,
    hide: true
  },
  {
    headerName: "Sport", field: "sport", filter: 'agSetColumnFilter', filterParams: {
      values: params => agGrid.simpleHttpRequest({url: 'http://localhost:9999/olympic-medals/getSports'})
        .then(data => {
          console.log(data);
          params.success(data)
        }),
      newRowsAction: 'keep'
    },
    enableRowGroup: true,
    enablePivot: true
  },
  { field: "gold", enableValue: true, aggFunc: 'sum' },
  { field: "silver", enableValue: true, aggFunc: 'sum' },
  { field: "bronze", enableValue: true, aggFunc: 'sum' },
  { field: "total", enableValue: true, aggFunc: 'sum' }
];

let gridOptions = {
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
  pivotPanelShow: 'always',
  animateRows: true,
  // icons: {
  //   groupLoading: '<img src="https://raw.githubusercontent.com/ag-grid/ag-grid-docs/master/src/javascript-grid-enterprise-model/spinner.gif" style="width:22px;height:22px;">'
  // }
};

function EnterpriseDatasource() {}

EnterpriseDatasource.prototype.getRows = function (params) {
  let request = modifyRequestForSetFilters(modifyRequestForGroupColumnSorting(params.request));

  let jsonRequest = JSON.stringify(request, null, 2);
  console.log(jsonRequest);

  let httpRequest = new XMLHttpRequest();
  httpRequest.open('POST', 'http://localhost:9999/olympic-medals/getData');
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

let modifyRequestForSetFilters = function (request) {
  request.filterModel = Object.keys(request.filterModel)
    .reduce(function (previous, current) {
      let currentFilter = request.filterModel[current];

      // convert set filter to common filter model format
      previous[current] = (currentFilter instanceof Array) ?
        {filterType: 'set', filter: null, filterTo: null, values: currentFilter} : currentFilter;

      return previous;
    }, {});
  return request;
}

let modifyRequestForGroupColumnSorting = function (request) {
  let sortModel = request.sortModel;
  let index = sortModel.findIndex(e => e.colId === 'ag-Grid-AutoColumn');

  if (index > -1) {
    let rowGroups = request.rowGroupCols.map(group => {
      return {
        colId: group.field,
        sort: sortModel[index].sort
      }
    });

    sortModel.splice.apply(sortModel, [index, 1].concat(rowGroups));
  }

  request.sortModel = sortModel;
  return request;
};

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