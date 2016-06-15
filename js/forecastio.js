(function () {
    console.log('Starting up our connector...');

    // Arrays for each of our tables' data
    var locationData = [], currentlyData = [], minutelyData = [], hourlyData = [], dailyData = [], alertsData = [];

    // --------------------------------------------------------------------------------
    // Create the connector object
    // --------------------------------------------------------------------------------
    var myConnector = tableau.makeConnector();

    // --------------------------------------------------------------------------------
    // Define our schema (from local .json file)
    // --------------------------------------------------------------------------------
    myConnector.getSchema = function(schemaCallback) {
        console.log('Entering getSchema()');
        
        $.getJSON("./js/forecastioSchema.json", function(schemaJson) {
            console.log('Fetching schema from a file')
            schemaCallback(schemaJson);
        });
    };
    
    // --------------------------------------------------------------------------------
    // Fetch our data
    //     This will be done for each table in our schema. Because we don't want to
    //     call the forecast API every time, get all the data once and then store table
    //     data in global arrays for each table.
    // --------------------------------------------------------------------------------
	myConnector.getData = function (table, doneCallback) {
        console.log('Entering getData() for ' + table.tableInfo.id);
        
        // Only call the forecast API for the location table (our default)
        // For all other tables we will append pre-processed data and then we will
        // delete all pre-processed data so that we don't inadvertently add that data
        // again if getData() is called more than once.    
        switch (table.tableInfo.id) {
            case "currently":
                table.appendRows(currentlyData);
                currentlyData = [];
                console.log('Appended data:  ' + table.tableInfo.id);
                doneCallback();
                break;
                
            case "minutely":
                table.appendRows(minutelyData);
                minutelyData = [];
                console.log('Appended data:  ' + table.tableInfo.id);
                doneCallback();
                break;
                
            case "hourly":
                table.appendRows(hourlyData);
                hourlyData = [];
                console.log('Appended data:  ' + table.tableInfo.id);
                doneCallback();
                break;
                
            case "daily":
                table.appendRows(dailyData);
                dailyData = [];
                console.log('Appended data:  ' + table.tableInfo.id);
                doneCallback();
                break;
            
            case "alerts":
                table.appendRows(alertsData);
                alertsData = [];
                console.log('Appended data:  ' + table.tableInfo.id);
                doneCallback();
                break;
        
            default:
                var locationRow = {};
                var currentlyRow = {};
                
                // forecast.io not set-up for CORS so using JSONP to get the data
                $.getJSON(tableau.connectionData + "?callback=?", function(data) {
                    // We've got data. Format it for all our tables.
                    if(data) {
                        console.log('Achievement unlocked! Data retrieved.');
                        
                        // Location Data
                        locationRow.latitude = data.latitude;
                        locationRow.longitude = data.longitude;
                        locationRow.timezone = data.timezone;
                        locationRow.offset = data.offset;
                        locationData.push(locationRow);
                        table.appendRows(locationData);
                        locationData = [];
                        console.log('Appended data:  ' + table.tableInfo.id);

                        // Currently Data
                        //     Prepare our data for currently since it includes
                        //     some summary fields from minutely, hourly, and daily
                        console.log('Starting to process Currently...');
                        console.log('Forecast.io returned the following for Currently: ' + JSON.stringify(data.currently));
                        currentlyRow = {
                            "time": dateToTableauDate(data.currently.time),
                            "summary": data.currently.summary,
                            "icon": data.currently.icon,
                            "minutelySummary": data.minutely.summary,
                            "minutelyIcon": data.minutely.icon,
                            "hourlySummary": data.hourly.summary,
                            "hourlyIcon": data.hourly.icon,
                            "dailySummary": data.daily.summary,
                            "dailyIcon": data.daily.icon,
                            "nearestStormDistance": data.currently.nearestStormDistance,
                            "nearestStormBearing": data.currently.nearestStormBearing,
                            "precipIntensity": data.currently.precipIntensity,
                            "precipProbability": data.currently.precipProbability,
                            "temperature": data.currently.temperature,
                            "apparentTemperature": data.currently.apparentTemperature,
                            "dewPoint": data.currently.dewPoint,
                            "humidity": data.currently.humidity,
                            "windSpeed": data.currently.windSpeed,
                            "windBearing": data.currently.windBearing,
                            "visibility": data.currently.visibility,
                            "cloudCover": data.currently.cloudCover,
                            "pressure": data.currently.pressure,
                            "ozone": data.currently.ozone
                        };
                        currentlyData.push(currentlyRow);
                        console.log('Finished processing Currently');
                        
                        // Store the rest of our tables' data so we can process it later
                        console.log('Now storing all other data...');
                        
                        // Minutely Data
                        console.log('Starting to process Minutely...');
                        for (var i = 0; i < data.minutely.data.length; i++) {
                            minutelyData.push(formatRow("minutely", data.minutely.data[i]));
                        }
                        console.log('Finished processing Minutely');
                        
                        // Hourly Data
                        console.log('Starting to process Hourly...');
                        for (var i = 0; i < data.hourly.data.length; i++) {
                            hourlyData.push(formatRow("hourly", data.hourly.data[i]));
                        }
                        console.log('Finished processing Hourly');
                        
                        // Daily Data
                        console.log('Starting to process Daily...');
                        for (var i = 0; i < data.daily.data.length; i++) {
                            dailyData.push(formatRow("daily", data.daily.data[i]));
                        }
                        console.log('Finished processing Daily');
                        
                        // Alerts
                        if (data.alerts) {
                            console.log('Starting to process Alerts...');
                            for (var i = 0; i < data.alerts.length; i++) {
                                alertsData.push(formatRow("alerts", data.alerts[i]));
                            }
                            console.log('Finished processing Alerts');
                        } else {
                            console.log('No alerts for this location.');
                        }
                        
                        doneCallback();
                    
                    } else {
                        tableau.log("error getting data");
                        tableau.abortWithError("error getting data from forecast.io");
                    }                    
                });
                
                break;
        }
	};

    // --------------------------------------------------------------------------------
    // Connector registration
    // --------------------------------------------------------------------------------
	tableau.registerConnector(myConnector);
    console.log('Called registerConnector()');

    // --------------------------------------------------------------------------------
    // Set Connection Data and Connection Name
    // --------------------------------------------------------------------------------
    myConnector.setLocation = function(latitude, longitude) {
      // Hard-code our Forecast.io API Key
      var apiKey = 'a328cf110f5d0b477e730b71c2cde8de';
      
      // Construct our URL for the API call
      var url = 'https://api.forecast.io/forecast/' + apiKey + '/' + latitude + ',' + longitude;
      
      tableau.connectionData = url;
      tableau.connectionName = 'Forecast.io Weather Data for ' + latitude + ' ' + longitude;
    }
    
    // --------------------------------------------------------------------------------
    // Utility functions
    // --------------------------------------------------------------------------------

    // formatRow
    //     Formats rows for the various tables per their schema
    function formatRow(table, inputRow) {
        var row = {};
        switch (table) {
            case "minutely":
                row = {
                    "time": dateToTableauDate(inputRow.time),
                    "precipIntensity": inputRow.precipIntensity,
                    "precipProbability": inputRow.precipProbability
                };
                break;

            case "hourly":
                row = {
                    "time": dateToTableauDate(inputRow.time),
                    "summary": inputRow.summary,
                    "icon": inputRow.icon,
                    "precipIntensity": inputRow.precipIntensity,
                    "precipProbability": inputRow.precipProbability,
                    "temperature": inputRow.temperature,
                    "apparentTemperature": inputRow.apparentTemperature,
                    "dewPoint": inputRow.dewPoint,
                    "humidity": inputRow.humidity,
                    "windSpeed": inputRow.windSpeed,
                    "windBearing": inputRow.windBearing,
                    "visibility": inputRow.visibility,
                    "cloudCover": inputRow.cloudCover,
                    "pressure": inputRow.pressure,
                    "ozone": inputRow.ozone
                };
                break;

            case "daily":
                row = {
                    "time": dateToTableauDate(inputRow.time),
                    "summary": inputRow.summary,
                    "sunriseTime": dateToTableauDate(inputRow.sunriseTime),
                    "sunsetTime": dateToTableauDate(inputRow.sunsetTime),
                    "moonPhase": inputRow.moonPhase,
                    "precipIntensity": inputRow.precipIntensity,
                    "precipIntensityMax": inputRow.precipIntensityMax,
                    "precipIntensityMaxTime": inputRow.precipIntensityMaxTime,
                    "precipProbability": inputRow.precipProbability,
                    "precipType": inputRow.precipType,
                    "temperatureMin": inputRow.temperatureMin,
                    "temperatureMinTime": dateToTableauDate(inputRow.temperatureMinTime),
                    "temperatureMax": inputRow.temperatureMax,
                    "temperatureMaxTime": dateToTableauDate(inputRow.temperatureMaxTime),
                    "apparentTemperatureMin": inputRow.apparentTemperatureMin,
                    "apparentTemperatureMinTime": dateToTableauDate(inputRow.apparentTemperatureMinTime),
                    "apparentTemperatureMax": inputRow.apparentTemperatureMax,
                    "apparentTemperatureMaxTime": dateToTableauDate(inputRow.apparentTemperatureMaxTime),
                    "dewPoint": inputRow.dewPoint,
                    "humidity": inputRow.humidity,
                    "windSpeed": inputRow.windSpeed,
                    "windBearing": inputRow.windBearing,
                    "visibility": inputRow.visibility,
                    "cloudCover": inputRow.cloudCover,
                    "pressure": inputRow.pressure,
                    "ozone": inputRow.ozone
                };
                break;
        
            default:                // alerts
                row = {
                    "title": inputRow.title,
                    "time": dateToTableauDate(inputRow.time),
                    "expires": dateToTableauDate(inputRow.expires),
                    "description": inputRow.description,
                    "uri": inputRow.uri
                };
                break;
        }
        return row;
    }
    
    // dataToTableauDate
    //     Use Moment.js to format a date to format required by Tableau
    function dateToTableauDate(dateToConvert) {
      // Use moment to convert dates to acceptible format for Tableau
      var tableauDate = moment.unix(dateToConvert).format("YYYY-MM-DD HH:mm:ss.SSS");   // Forecast.io timestaps are unix
      
      return tableauDate;
    }

    function TODO_DELETE() {
        return false;
        // function fetchForecast() {
        //     var locationRow = {};
        //     console.log('fetchForecast');
            
        //     // local...
        //     // $.getJSON("js/forecastData.json", function(data) {

            
        //     // forecast.io not set-up for CORS so using JSONP to get the data
        //     $.getJSON(tableau.connectionData + "?callback=?", function(data) {
        //         // We've got data so format & send to Tableau
        //         if(data) {
        //             console.log('We have data!!!!');
        //             console.log(JSON.stringify(data));
                    
        //             // TODO use this for other tables -> var daily = data.daily.data;
        //             locationRow.latitude = data.latitude;
        //             locationRow.longitude = data.longitude;
        //             locationRow.timezone = data.timezone;
        //             locationRow.offset = data.offset;
                    
        //             locationData.push(locationRow);
        //             console.log('Location data done...');
                    
        //             // // Currently Data
        //             // console.log('Starting to process Currently...');
        //             // processCurrentlyData(data.currently);
        //             // console.log('Finished processing Currently');
                    
        //             // // Minutely Data
        //             // console.log('Starting to process Minutely...');
        //             // processMinutelyData(data.minutely.data);
        //             // console.log('Finished processing Minutely');
                    
        //             // // Hourly Data
        //             // console.log('Starting to process Hourly...');
        //             // processHourlyData(data.hourly.data);
        //             // console.log('Finished processing Hourly');
                    
        //             // // Daily Data
        //             // console.log('Starting to process Daily...');
        //             // processDailyData(data.daily.data);
        //             // console.log('Finished processing Daily');
                    
        //             // // Alerts
        //             // console.log('Starting to process Alerts...');
        //             // if (data.alerts) {
        //             //     processAlerts(data.alerts);
        //             // }
        //             // console.log('Finished processing Alerts');
                    
        //             console.log('Returning locationData');
        //             return locationData;
                
        //         } else {
        //             tableau.log("error getting data");
        //             tableau.abortWithError("error getting data from forecast.io");
        //         }
        //     });
        // }
    }
    
    // --------------------------------------------------------------------------------
    // HTML page event handlers
    // --------------------------------------------------------------------------------
    $(document).ready(function(){
        $("#submitButton").click(function() {
            var latitude = $('#latitude').val();
            var longitude = $('#longitude').val();
            latitude = latitude.trim();
            longitude = longitude.trim();

            // Make sure we have values before we proceed
            if (!latitude || !longitude) {
                console.log('Must have lat/long to fetch data!');
                return;
            } else {
                myConnector.setLocation(latitude, longitude);
                tableau.submit();
            }
        });
    });
})();