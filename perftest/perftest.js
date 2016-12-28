/* globals d3, $, _ */

$(window).ready(function () {
  var shas = window.location.hash.substr(1).split('..');
  var $gets = [];

  //var params = val.params;
  //var results = val.results;
  var testsDiv = d3.select("#tests");
  var data = {};
  var dataPerSha = [];

  // Generate XHR promises for each SHA
  $.each(shas, function (i, sha) {
    $gets.push($.getJSON(sha + '.json'));
  });

  // populate graphs with data when it's all loaded
  $.when.apply($.when, $gets).done(function () {
    if (shas.length === 1) {
      dataPerSha.push({
        sha: shas[0],
        color: 0,
        tests: arguments[0]
      });
    } else {
      for (var i = 0 ; i < arguments.length ; i += 1) {
        dataPerSha.push({
          sha: shas[i],
          color: i,
          tests: arguments[i][0]
        });
        $('#colors').append($('<div >').text(shas[i]).addClass(
          'color_' + i));
      }
    }
    for (var i = 0; i < dataPerSha.length ; i += 1) {
      var d = dataPerSha[i];
      $.each(d.tests, function (key, test) {
        if (!_.has(data, key)) {
          data[key] = {
            params: test.params,
            results: [],
          };
        }
        for (var j = 0; j < test.results.length ; j += 1) {
          var r = test.results[j];
          r.sha = d.sha;
          r.color = d.color,
          data[key].results.push(r);
        }
      });
    }

    data = _.map(data, function (val, key) {
      return {
        testName: key,
        params: val.params,
        results: val.results
      };
    });

    var svgWidth = 150;
    var svgHeight = 150;

    var chartEnter = testsDiv.data(data).enter()
                             .append('div')
                             .attr('class', 'chart');

    var legend = chartEnter.append('div').attr('class', 'legend')
                           .text(function (d) {
                             return d.params.api_method + ', ' +
                                 d.params.geom_complexity + ', ' +
                                 d.params.geom + ', ' +
                                 d.params.normalization + ', ' +
                                 d.params.boundary;
                           });
    var svgEnter = chartEnter.append("svg")
                             .attr("width", svgWidth)
                             .attr("height", svgHeight);


    var x = d3.local();
    var y = d3.local();
    var margin = {top: 20, right: 20, bottom: 30, left: 40},
        width = +svgWidth - margin.left - margin.right,
        height = +svgHeight - margin.top - margin.bottom;

    var svg = testsDiv.selectAll('svg')
                      .data(function (d) {
                        return d.results;
                      });

    var g = svgEnter.append("g")
        .each(function (d) {
          x.set(this, d3.scaleBand().rangeRound([0, width]).padding(0.1))
                        .domain(_.pluck(d.results, 'rows'));
          y.set(this, d3.scaleLinear().rangeRound([height, 0]))
                        .domain([0, d3.max(d.results, function (d) {
                          return d.qps; })]);
        })
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    g.append("g")
        .attr("class", "axis axis--x")
        .attr("transform", "translate(0," + height + ")")
        .each(function () {
          d3.axisBottom(x.get(this))(d3.select(this));
        });

    g.append("g")
        .attr("class", "axis axis--y")
        .each(function () {
          d3.axisLeft(y.get(this)).ticks(4)(d3.select(this));
        })
        .append("text")
        .attr("transform", "rotate(-90)")
        .attr("y", 6)
        .attr("dy", "0.71em")
        .attr("text-anchor", "end")
        .text("QPS");

    g.selectAll(".bar")
      .data(function (d) {
        return d.results;
      })
      .enter()
      .append("rect")
      .attr("class", function (d) {
        return 'bar color_' + d.color;
      })
      .attr("x", function(d) {
        return x.get(this)(d.rows) +
          ((x.get(this).bandwidth() / dataPerSha.length) * d.color);
      })
      .attr("y", function(d) {
        return y.get(this)(d.qps);
      })
      .attr("width", function () {
        return x.get(this).bandwidth() / dataPerSha.length;
      }).attr("height", function(d) {
        return height - y.get(this)(d.qps);
      });

  });
});
