// module targets: nodejs & browserify
'use strict';

var http = require('http');
var https = require('https');
var querystring = require('querystring');
var _ = require('lodash');
var Q = require('q');
var assert = require('assert');
var URL = require('url');
var PATH = require('path');
var Cookie = require('cookie-jar');
var tv4 = require('tv4').freshApi();


module.exports = restful_api;

// TODO better keep cookie jars in a client object, but now the client objects are per api
var cookie_jars_per_host = {};

var VALID_METHODS = {
    GET: 1,
    PUT: 1,
    POST: 1,
    DELETE: 1
};
var PATH_ITEM_RE = /^\S*$/;


// Check and initialize the api structure.
//
// api (Object):
// - each key is func_name (String)
// - each value is func_info (Object):
//   - method (String) - http method GET/POST/...
//   - path (Function) - function(params) that returns the path (String) for the call
//   - data (Function) - function(params) that returns the data (String|Buffer) for the call
//
function restful_api(api) {

    // client class for the api.
    // creating a client instance takes client_params,
    // which is needed for when doing the actual calls.
    //
    // client_params (Object):
    // - hostname (String)
    // - port (Number)
    // - path (String) - base path for the host
    //
    function Client(client_params) {
        this._restful_client_params = client_params || {};
    }

    Client.prototype.set_param = function(key, value) {
        this._restful_client_params[key] = value;
    };

    // server class for the api.
    //
    // methods (Object): map of function names to function(params).
    //
    // allow_missing_methods (String):
    //    call with allow_missing_methods==='allow_missing_methods' to make the server
    //    accept missing functions, the handler for missing functions will fail on runtime.
    //    useful for test servers.
    //
    function Server(methods, middlewares, allow_missing_methods) {
        var self = this;
        if (allow_missing_methods) {
            assert.strictEqual(allow_missing_methods, 'allow_missing_methods');
        }
        self._middlewares = middlewares || [];
        self._impl = {};
        self._handlers = {};
        _.each(api.methods, function(func_info, func_name) {
            var func = methods[func_name];
            if (!func && allow_missing_methods) {
                func = function(params) {
                    return Q.reject({
                        data: 'RESTFUL_API: missing method implementation - ' + func_info.fullname
                    });
                };
            }
            assert.strictEqual(typeof(func), 'function',
                'RESTFUL_API: server method should be a function - ' + func_info.fullname);
            self._impl[func_name] = func;
            self._handlers[func_name] = create_server_handler(self, func, func_info);
        });
    }

    // install the server handlers to the given router.
    //
    // router (Object) - express/connect style app router with the following functions:
    // - get,post,put,delete which are function(path, handler).
    //
    // base_path (String) - optional base path for the routes.
    //
    Server.prototype.install_routes = function(router, base_path) {
        var self = this;
        base_path = base_path || '';
        var doc_base = PATH.join(base_path, 'doc', api.name);
        _.each(self._middlewares, function(fn) {
            assert(fn, 'RESTFUL_API: undefined middleware function');
            router.use(base_path, function(req, res, next) {
                Q.fcall(fn, req).done(function() {
                    return next();
                }, function(err) {
                    return next(err);
                });
            });
        });
        _.each(api.methods, function(func_info, func_name) {
            // install the path handler
            var path = PATH.join(base_path, func_info.path);
            var handler = self._handlers[func_name];
            install_route(router, func_info.method, path, handler);

            // install also a documentation route
            router.get(PATH.join(doc_base, func_name), function(req, res) {
                res.send(func_info.doc);
                // TODO docs should return also params/reply/other-info doc
            });
        });
    };

    // call to bypass the server routes
    Server.prototype.disable_routes = function() {
        this._disabled = true;
    };

    // call to start logging the server requests
    Server.prototype.set_logging = function() {
        this._log = console.log.bind(console);
    };


    // add all definitions
    _.each(api.definitions, function(schema, name) {
        tv4.addSchema('/' + api.name + '/definitions/' + name, schema);
    });

    // go over the api and check its validity
    var method_and_path_collide = {};
    _.each(api.methods, function(func_info, func_name) {
        // add the name to the info
        func_info.name = func_name;
        func_info.fullname = '/' + api.name + '/methods/' + func_name;
        func_info.params_schema = func_info.fullname + '/params';
        func_info.reply_schema = func_info.fullname + '/reply';

        tv4.addSchema(func_info.params_schema, func_info.params || {});
        tv4.addSchema(func_info.reply_schema, func_info.reply || {});
        func_info.params_properties = tv4.getSchema(func_info.params_schema).properties;

        assert(func_info.method in VALID_METHODS,
            'RESTFUL_API: unexpected http method: ' +
            func_info.method + ' for ' + func_info.fullname);

        assert.strictEqual(typeof(func_info.path), 'string',
            'RESTFUL_API: unexpected path type: ' +
            func_info.path + ' for ' + func_info.fullname);

        // split the path to its items
        func_info.path_items = _.map(func_info.path.split('/'), function(p) {
            assert(PATH_ITEM_RE.test(p),
                'RESTFUL_API: invalid path item: ' + p + ' for ' + func_info.fullname);

            // if a normal path item, just return the string
            if (p[0] !== ':') {
                return p;
            }
            // if a param item (starts with colon) find the param info
            p = p.slice(1);
            var param = func_info.params_properties[p];
            assert(param, 'RESTFUL_API: missing param info: ' + p + ' for ' + func_info.fullname);
            return {
                name: p,
                param: param,
            };
        });

        // test for colliding method+path
        var method_and_path = func_info.method + func_info.path;
        var collision = method_and_path_collide[method_and_path];
        assert(!collision, 'RESTFUL_API: collision of method+path: ' +
            func_info.name + ' ~ ' + collision);
        method_and_path_collide[method_and_path] = func_info.name;

        // set the client class prototype functions
        Client.prototype[func_name] = function(params) {
            // resolve this._restful_client_params to use the client object
            return do_client_request(this._restful_client_params, func_info, params);
        };
    });

    // add the client and server classes to the api object
    api.Client = Client;
    api.Server = Server;

    return api;
}



// call a specific REST api function over http request.
function do_client_request(client_params, func_info, params) {
    return Q.fcall(function() {
        // first prepare the request
        return create_client_request(client_params, func_info, params);
    }).then(function(options) {
        // now send it over http
        return send_http_request(options);
    }).then(function(res) {
        var cookies = res.response.headers['set-cookie'];
        if (cookies) {
            var host = client_params.host || (client_params.hostname + ':' + client_params.post);
            var jar = cookie_jars_per_host[host] = cookie_jars_per_host[host] || new Cookie.Jar();
            _.each(cookies, function(cookie_str) {
                jar.add(new Cookie(cookie_str));
            });
        }
        if (!func_info.reply_raw) {
            // check the json reply
            validate_schema(res.data, func_info.reply_schema, func_info, 'client reply');
        }
        return res.data;
    }).then(null, function(err) {
        console.error('RESTFUL REQUEST FAILED', err);
        throw err;
    });
}


// create a REST api call and return the options for http request.
function create_client_request(client_params, func_info, params) {
    var method = func_info.method;
    var path = client_params.path || '/';
    var data = _.clone(params) || {};
    var headers = _.clone(client_params.headers) || {};
    var body;
    if (func_info.param_raw) {
        body = data[func_info.param_raw];
        headers['content-type'] = 'application/octet-stream';
        headers['content-length'] = body.length;
        delete data[func_info.param_raw];
    }
    validate_schema(data, func_info.params_schema, func_info, 'client request');
    // construct the request path for the relevant params
    _.each(func_info.path_items, function(p) {
        if (!p) {
            return;
        } else if (typeof(p) === 'string') {
            // for plain path strings which are non params
            path = PATH.join(path, p);
        } else {
            assert(p.name in params,
                'RESTFUL_API: missing required path param: ' +
                p + ' for ' + func_info.fullname);
            path = PATH.join(path, param_to_component(data[p.name], p.param.type));
            delete data[p.name];
        }
    });
    headers.accept = '*/*';
    var host = client_params.host || (client_params.hostname + ':' + client_params.post);
    var jar = cookie_jars_per_host[host];
    if (jar) {
        headers.cookie = jar.cookieString({
            url: path
        });
    }
    if (!func_info.param_raw && (method === 'POST' || method === 'PUT')) {
        body = JSON.stringify(data);
        headers['content-type'] = 'application/json';
        headers['content-length'] = body.length;
    } else {
        // when func_info.param_raw or GET, HEAD, DELETE we can't use the body,
        // so encode the data into the path query
        _.each(data, function(v, k) {
            data[k] = param_to_component(data[k], func_info.params_properties[k].type);
        });
        var query = querystring.stringify(data);
        if (query) {
            path += '?' + query;
        }
    }
    var options = {
        protocol: client_params.protocol,
        hostname: client_params.hostname,
        port: client_params.port,
        method: method,
        path: path,
        headers: headers,
        body: body,
    };
    return options;
}


// send http request and return a promise for the response
function send_http_request(options) {
    var defer = Q.defer();
    // console.log('HTTP request', options);
    var protocol = options.protocol;
    var body = options.body;
    options = _.omit(options, 'body', 'protocol');
    var req = protocol === 'https' ?
        https.request(options) :
        http.request(options);

    req.on('response', function(res) {
        // console.log('HTTP response headers', res.statusCode, res.headers);
        var chunks = [];
        var chunks_length = 0;
        var response_err;

        res.on('data',
            function(chunk) {
                // console.log('HTTP response data', chunk);
                chunks.push(chunk);
                chunks_length += chunk.length;
            }
        );

        res.on('error',
            function(err) {
                // console.log('HTTP response error', err);
                response_err = response_err || err;
            }
        );

        res.on('end',
            function() {
                var data = chunks_length ? Buffer.concat(chunks, chunks_length) : null;
                // console.log('HTTP response end', res.statusCode, response_err, data);
                if (data && data.length) {
                    var content_type = res.headers['content-type'];
                    if (content_type &&
                        content_type.split(';')[0] === 'application/json') {
                        try {
                            data = JSON.parse(data.toString('utf8'));
                        } catch (err) {
                            response_err = response_err || err;
                        }
                    }
                }
                if (res.statusCode !== 200 || response_err) {
                    return defer.reject({
                        status: res.statusCode,
                        data: response_err || data,
                    });
                } else {
                    return defer.resolve({
                        response: res,
                        data: data,
                    });
                }
            }
        );
    });

    req.on('error',
        function(err) {
            // console.log('HTTP request error', err);
            return defer.reject({
                data: err,
            });
        }
    );

    if (body) {
        req.write(body);
    }
    req.end();
    return defer.promise;
}




// return a route handler that calls the server function
function create_server_handler(server, func, func_info) {
    return function(req, res, next) {
        // marking _disabled on the server will bypass all the routes it has.
        if (server._disabled) {
            return next();
        }
        var log_func = server._log || function() {};
        Q.fcall(
            function() {
                req.restful_params = {};
                _.each(req.query, function(v, k) {
                    req.restful_params[k] =
                        component_to_param(v, func_info.params_properties[k].type);
                });
                if (!func_info.param_raw) {
                    _.each(req.body, function(v, k) {
                        req.restful_params[k] = v;
                    });
                }
                _.each(req.params, function(v, k) {
                    req.restful_params[k] =
                        component_to_param(v, func_info.params_properties[k].type);
                });
                validate_schema(req.restful_params, func_info.params_schema, func_info, 'server request');
                if (func_info.param_raw) {
                    req.restful_params[func_info.param_raw] = req.body;
                }
                // server functions are expected to return a promise
                return func(req, res, next);
            }
        ).then(
            function(reply) {
                log_func('SERVER COMPLETED', func_info.name);
                if (func_info.reply_raw) {
                    res.set('content-type', 'application/octet-stream');
                    res.set('content-length', reply.length);
                    return res.status(200).send(reply);
                } else {
                    validate_schema(reply, func_info.reply_schema, func_info, 'server reply');
                    return res.status(200).json(reply);
                }
            }
        ).then(null,
            function(err) {
                log_func('SERVER ERROR', func_info.name, ':', err, err.stack);
                var status = err.status || err.statusCode;
                var data = err.data || err.message || err.toString();
                if (typeof status === 'number' &&
                    status >= 100 &&
                    status < 600
                ) {
                    return res.status(status).json(data);
                } else {
                    return res.status(500).json(data);
                }
            }
        ).done(null,
            function(err) {
                log_func('SERVER ERROR WHILE SENDING ERROR', func_info.name, ':', err, err.stack);
                return next(err);
            }
        );
    };
}


// install a route handler for the given router.
// see install_routes().
function install_route(router, method, path, handler) {
    // route_func points to the route functions router.get/post/put/delete
    var route_func = router[method.toLowerCase()];
    // call the route function to set the route handler
    route_func.call(router, path, handler);
}


tv4.addFormat('date', function(data) {
    var d = new Date(data);
    return isNaN(d.getTime()) ? 'bad date' : null;
});


function validate_schema(obj, schema, info, desc) {
    var result = tv4.validateResult(
        obj, schema,
        true /*checkRecursive*/ ,
        true /*banUnknownProperties*/ );
    if (!result.valid) {
        console.error('INVALID SCHEMA', desc, schema, obj);
        result.info = info;
        result.desc = desc;
        throw result;
    }
}

function param_to_component(param, type) {
    if (type === 'array' || type === 'object') {
        return encodeURIComponent(JSON.stringify(param));
    } else {
        return param.toString();
    }
}

function component_to_param(component, type) {
    if (type === 'array' || type === 'object') {
        if (typeof(component) === type) {
            return component; // already parsed
        } else {
            return JSON.parse(decodeURIComponent(component));
        }
    } else if (type === 'integer') {
        return Number(component) | 0;
    } else if (type === 'number') {
        return Number(component);
    } else if (type === 'boolean') {
        return Boolean(component);
    } else if (type === 'string') {
        return String(component);
    } else {
        return component;
    }
}
