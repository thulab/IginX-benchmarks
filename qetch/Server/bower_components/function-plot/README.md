# function-plot 

[![NPM][npm-image]][npm-url]
[![Build Status][travis-image]][travis-url]
[![Stability](https://img.shields.io/badge/stability-stable-green.svg)](https://nodejs.org/api/documentation.html#apicontent)

[![js-standard-style](https://cdn.rawgit.com/feross/standard/master/badge.svg)](https://github.com/feross/standard)

> A  2d function plotter powered by d3

Function Plot is a powerful library built on top of <a href="http://d3js.org/">D3.js</a> whose purpose
is to render functions with little configuration (think of it as a little clone of Google's plotting
utility: [y = x * x](https://www.google.com/webhp?sourceid=chrome-instant&ion=1&espv=2&es_th=1&ie=UTF-8#q=y+%3D+x+%5E+2)

The library currently supports interactive line charts and scatterplots, whenever the graph scale is modified the function
is evaluated again with the new bounds, result: infinite graphs!

Have a look at [the homepage](http://maurizzzio.github.io/function-plot/)

## Install

```sh
$ npm install --save function-plot
```

## Usage with browserify

```js
var d3 = window.d3
var functionPlot = require('function-plot');
functionPlot({
  // options below
})
```

## Example

All the available options are described in the [`homepage`](http://maurizzzio.github.io/function-plot/)

## API

```
var functionPlot = require('function-plot');
```

### `instance = functionPlot(options)`

**params, All the params are optional unless otherwise stated**

* `options`
  * `options.target` {string} the selector of the parent element to render the graph to
  * `options.title` {string} If set the chart will have it as a title on the top
  * `options.xDomain` {array} domain of the linear scale (used in the x axis) 
  * `options.yDomain` {array} domain of the linear scale (used in the y axis)
  * `options.xLabel` {string} x axis label 
  * `options.yLabel` {string} y axis label
  * `options.disableZoom` {boolean} true to disable drag and zoom on the graph
  * `options.grid` {boolean} true to show a grid
  * `options.tip` {object} configuration passed to `lib/tip`, it's the helper shown on mouseover on the closest
  function to the current mose position
    * `options.tip.xLine` {boolean} true to show a line parallel to the X axis on mouseover
    * `options.tip.yLine` {boolean} true to show a line parallel to the Y axis on mouseover
    * `options.tip.renderer` {function} Function to be called to define custom rendering on mouseover, called with the
     `x` and `f(x)` of the function which is closest to the mouse position (args: `x, y`)
  * `options.annotations` {array} An array defining parallel lines to the y-axis or the x-axis
    * `options.annotations[i].x` {number} x-coordinate of the line parallel to the y-axis
    * `options.annotations[i].y` {number} y-coordinate of the line parallel to the x-axis
    * `options.annotations[i].text` {string} text shown next to the parallel line    
  * `options.data` {array} *required* An array defining the functions to be rendered
    * `options.data[i].title` {string} title of the function
    * `options.data[i].color` {string} color of the function
    * `options.data[i].skipTip` {boolean} true to avoid this function from being a target of the tip
    * `options.data[i].fn` *required* {string} the function that represents the curve, this function is evaluated 
    with values which are in `range` limiting the values to the screen min/max coordinates for `x`, i.e.
    at any given time the graph min/max x coordinates will limit the range of values to be plotted
    * `options.data[i].range` {number[]} if given the function will only be evaluated with multiple values from this range
    * `options.data[i].samples` {number} the fixed number of samples to be computed within the current domain ends
    * `options.data[i].implicit` {boolean} true to creates samples for the function considering it implicit, it assumes
    that the function depends on the variables *x* and *y*
    * `options.data[i].secants` {Object[]} Secants of `options.data[i].fn`
      * `options.data[i].secants[j].x0` {number} The abscissa of the first point
      * `options.data[i].secants[j].x1` {number} (optional if `updateOnMouseMove` is set) The abscissa of the second point
      * `options.data[i].secants[j].updateOnMouseMove` {boolean} (optional) True to update the secant line by evaluating
      `options.data[i].fn` with the current mouse position (`x0` is the fixed point and `x1` is computed
      dynamically based on the current mouse position)
    * `options.data[i].derivative` {Object} Info of the instantaneous rate of change of y with respect to x
      * `options.data[i].derivative.fn` {string} The derivative of `options.data[i].fn`
      * `options.data[i].derivative.x0` {number} The abscissa of the point which belongs to the curve
      represented by `options.data[i].fn` whose tangent will be computed (i.e. the tangent line to the point
      `x0, fn(x0)`)
      * `options.data[i].derivative.updateOnMouseMove` {boolean} True to compute the tangent line by evaluating
      `options.data[i].derivative.fn` with the current mouse position (i.e. let `x0` be the abscissa of the
      mouse position transformed to local coordinates, the tangent line to the point `x0, fn(x0)`)
    * `options.data[i].graphOptions` {Object} options passed to the the files located in `lib/type/` which plot the data
    generated by the library, the most useful property of this object is `type` which is used to determine the type of
    graph to be rendered for a function
  * `options.plugins` {array} An array describing plugins to be run when the graph is initialized, check out the
    examples on the main page
    
### `options.data[i].graphOptions` {Object}

* `type` {string} type of graph, currently `line`, `scatter` and `interval` are supported, `interval` is the default option
* `sampler` {string} the sampler to use, currently `interval` and `builtIn` are supported (`interval` should
be used with `type: 'interval'` and `builtIn` with `type: 'line'` or `type: 'scatter'`)

Depending on the type option:

* `options.closed` {boolean} (only if `type: 'interval'` or `type: 'line'`) True to close the path, for any segment of the closed area graph
  `y0` will be 0 and `y1` will be `f(x)`  
  
#### if `options.data[i].parametric = true`

- `x` the x-coordinate of a point to be sampled with a parameter `t`
- `y` the y-coordinate of a point to be sampled with a parameter `t`
- `range = [0, 2 * Math.PI]` the `range` property in parametric equations is used
to determine the possible values of `t`, remember that the number of samples is
set in the property `samples`

#### if `options.data[i].polar = true`

- `r` a polar equation in terms of `theta`
- `range = [-Math.PI, Math.PI]` the `range` property in polar equations is used
to determine the possible values of `theta`, remember that the number of samples is
set in the property `samples`
   
#### if `options.data[i].implicit = true`

- `fn` a function which needs to be expressed in terms of `x` and `y`

### `instance`  

* `instance.id` {string} a random generated id made out of letters and numbers
* `instance.linkedGraphs` {array} array of function-plot instances linked to the events of this instance,
i.e. when the zoom event is dispatched on this instance it's also dispatched on all the instances of
this array
* `instance.meta` {object}
  * `instance.meta.margin` {object} graph's left,right,top,bottom margins
  * `instance.meta.width` {number} width of the canvas (minus the margins)
  * `instance.meta.height` {number} height of the canvas (minus the margins)
  * `instance.meta.xScale` {d3.scale.linear} graph's x-scale
  * `instance.meta.yScale` {d3.scale.linear} graph's y-scale
  * `instance.meta.xAxis` {d3.svg.axis} graph's x-axis
  * `instance.meta.yAxis` {d3.svg.axis} graph's y-axis
* `instance.root` {d3.selection} `svg` element that holds the graph (canvas + title + axes)
* `instance.canvas` {d3.selection} `g.canvas` element that holds the area where the graphs are plotted
(clipped with a mask)

**Events**

An instance can subscribe to any of the following events by doing `instance.on([eventName], callback)`,
events can be triggered by doing `instance.emit([eventName][, params])`

* `mouseover` fired whenever the mouse is over the canvas
* `mousemove` fired whenever the mouse is moved inside the canvas, callback params `x`, `y` (in canvas space
coordinates)
* `mouseout` fired whenever the mouse is moved outside the canvas
* `before:draw` fired before drawing all the graphs 
* `after:draw` fired after drawing all the graphs 
* `zoom:scaleUpdate` fired whenever the scale of another graph is updated, callback params `xScale`, `yScale`
(x-scale and y-scale of another graph whose scales were updated)
* `tip:update` fired whenever the tip position is updated, callback params `x`, `y`, `index` (in canvas
space coordinates, `index` is the index of the graph where the tip is on top of)
* `eval` fired whenever the sampler evaluates a function, callback params `data` (an array of segment/points),
`index` (the index of datum in the `data` array), `isHelper` (true if the data is created for a helper e.g.
for the derivative/secant)

The following events are dispatched to all the linked graphs

* `all:mouseover` same as `mouseover` but it's dispatched in each linked graph
* `all:mousemove` same as `mousemove` but it's dispatched in each linked graph
* `all:mouseout` same as `mouseout` but it's dispatched in each linked graph
* `all:zoom:scaleUpdate` same as `zoom:scaleUpdate` but it's dispatched in each linked graph
* `all:zoom` fired whenever there's scaling/translation on the graph, dispatched on all the linked graphs

When the `definite-integral` plugin is included the instance will fire the following events

* `definite-integral`
  * `datum` {object} The datum whose definite integral was computed
  * `i` {number} The index of the datum in the `data` array
  * `value` {number} The value of the definite integral
  * `a` {number} the left endpoint of the interval
  * `b` {number} the right endpoint of the interval

## Recipes

### Evaluate a function at some value `x`
 
```javascript
var y = functionPlot.eval.builtIn(datum, fnProperty, scope)
```

Where `datum` is an object that has a function to be evaluated in the property `fnProperty` ,
to eval this function we need an `x` value which is sent through the scope

e.g.

```javascript
var datum = {
  fn: 'x^2'
}
var scope = {
  x: 2
}
var y = functionPlot.eval.builtIn(datum, 'fn', scope)
```

Every element of the `data` property sent to `functionPlot` is saved on `instance.options.data`,
if you want to get the evaluated values of all the elements here run

```javascript
var instance = functionPlot( ... )
instance.options.data.forEach(function (datum) {
  var datum = {
    fn: 'x^2'
  }
  var scope = {
    // a value for x
    x: 2
  }
  var y = functionPlot.eval.builtIn(datum, 'fn', scope)
}
```

### Programmatic zoom

Just call `instance.programmaticZoom` with the desired `x` and `y` domains

```javascript
var instance = functionPlot( ... )
var xDomain = [-3, 3]
var yDomain = [-1.897, 1.897]
instance.programmaticZoom(xDomain, yDomain)
```

### Maintain aspect ratio

Given the `xDomain` values you can compute the corresponding `yDomain` values to main
the aspect ratio between the axes

```javascript
function computeYScale (width, height, xScale) {
  var xDiff = xScale[1] - xScale[0]
  var yDiff = height * xDiff / width
  return [-yDiff / 2, yDiff / 2]
}

var width = 800
var height = 400

// desired xDomain values
var xScale = [-10, 10]

functionPlot({
  width: width,
  height: height,
  xDomain: xScale,
  yDomain: computeYScale(width, height, xScale),

  target: '#demo',
  data: [{
    fn: 'x^2',
    derivative: {
      fn: '2x',
      updateOnMouseMove: true
    }
  }]
})
```

### Changing the format of the values shown on the axes

```javascript
var instance = functionPlot({
  target: '#complex-plane',
  xLabel: 'real',
  yLabel: 'imaginary'
})
// old format
var format = instance.meta.yAxis.tickFormat()
var imaginaryFormat = function (d) {
  // new format = old format + ' i' for imaginary
  return format(d) + ' i'
}
// update format
instance.meta.yAxis.tickFormat(imaginaryFormat)
// redraw the graph
instance.draw()
```

### Styling

Selectors (sass)

```sass
.function-plot {
  .x.axis {
    .tick {
      line {
        // grid's vertical lines
      }
      text {
        // x axis labels
      }
    }
    path.domain {
      // d attribute defines the graph bounds
    }
  }
  
  .y.axis {
    .tick {
      line {
        // grid's horizontal lines
      }
      text {
        // y axis labels
      }
    }
    path.domain {
      // d attribute defines the graph bounds
    }
  }
}
```

## Development

After cloning the repo and running `npm install`

```sh
node site.js    // generate the examples shown on index.html
npm start
```

Open `127.0.0.1:5555` and that's it! Local development server powered [beefy](https://www.npmjs.com/package/beefy)

Plain demo: `127.0.0.1:5555/demo.html` 

## License

2015 MIT ?? Mauricio Poppe

[npm-image]: https://img.shields.io/npm/v/function-plot.svg?style=flat
[npm-url]: https://npmjs.org/package/function-plot
[travis-image]: https://travis-ci.org/maurizzzio/function-plot.svg?branch=master
[travis-url]: https://travis-ci.org/maurizzzio/function-plot
