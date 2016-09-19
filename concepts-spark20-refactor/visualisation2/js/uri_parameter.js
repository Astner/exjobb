
var uri_parameter= (function(){

// Get named parameter from top-level URI
    function get(param, asArray) {
      return window
               .parent
               .location
               .search
               .substring(1)
               .split('&')
               .reduce(function(p,c) {
                            var parts = c.split('=', 2)
                                         .map(function(param) {return decodeURIComponent(param);});
                            if(parts.length == 0 || parts[0] != param) {
                                return (p instanceof Array) && !asArray ? null : p;
                            }
                        return asArray ? p.concat(parts.concat(true)[1]) : parts.concat(true)[1];
                       }, []);
    }

    return {get: get};
    
}());

console.log("*** " 
            + document.getElementsByTagName('script')[0].baseURI 
            + " loaded: uri_parameter.js" 
            +  " ***");
