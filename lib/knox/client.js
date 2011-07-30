
/*!
 * knox - Client
 * Copyright(c) 2010 LearnBoost <dev@learnboost.com>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var utils = require('./utils')
  , auth = require('./auth')
  , http = require('http')
  , url = require('url')
  , join = require('path').join
  , mime = require('./mime')
  , fs = require('fs')
  , crypto = require('crypto');

/**
 * Initialize a `Client` with the given `options`.
 *
 * Required:
 *
 *  - `key`     amazon api key
 *  - `secret`  amazon secret
 *  - `bucket`  bucket name string, ex: "learnboost"
 *
 * @param {Object} options
 * @api public
 */

var Client = module.exports = exports = function Client(options) {
  this.endpoint = 's3.amazonaws.com';
  this.port = 80;
  if (!options.key) throw new Error('aws "key" required');
  if (!options.secret) throw new Error('aws "secret" required');
  if (!options.bucket) throw new Error('aws "bucket" required');
  utils.merge(this, options);
};

/**
 * Request with `filename` the given `method`, and optional `headers`.
 *
 * @param {String} method
 * @param {String} filename
 * @param {Object} headers
 * @return {ClientRequest}
 * @api private
 */

Client.prototype.request = function(method, filename, headers){
  var options = { host: this.endpoint, port: this.port }
    , date = new Date
    , headers = headers || {};

  // Default headers
  utils.merge(headers, {
      Date: date.toUTCString()
    , Host: this.endpoint
  });

  // Authorization header
  headers.Authorization = auth.authorization({
      key: this.key
    , secret: this.secret
    , verb: method
    , date: date
    , resource: auth.canonicalizeResource(join('/', this.bucket, filename))
    , contentType: headers['Content-Type']
    , md5: headers['Content-MD5'] || ''
    , amazonHeaders: auth.canonicalizeHeaders(headers)
  });

  // Issue request
  options.method = method;
  options.path = join('/', this.bucket, filename);
  options.headers = headers;
  var req = http.request(options);
  req.url = this.url(filename);

  return req;
};

/**
 * PUT data to `filename` with optional `headers`.
 *
 * Example:
 *
 *     // Fetch the size
 *     fs.stat('Readme.md', function(err, stat){
 *      // Create our request
 *      var req = client.put('/test/Readme.md', {
 *          'Content-Length': stat.size
 *        , 'Content-Type': 'text/plain'
 *      });
 *      fs.readFile('Readme.md', function(err, buf){
 *        // Output response
 *        req.on('response', function(res){
 *          console.log(res.statusCode);
 *          console.log(res.headers);
 *          res.on('data', function(chunk){
 *            console.log(chunk.toString());
 *          });
 *        });
 *        // Send the request with the file's Buffer obj
 *        req.end(buf);
 *      });
 *     });
 *
 * @param {String} filename
 * @param {Object} headers
 * @return {ClientRequest}
 * @api public
 */

Client.prototype.put = function(filename, headers){
  headers = utils.merge({
      Expect: '100-continue'
    , 'x-amz-acl': 'public-read'
  }, headers || {});
  return this.request('PUT', filename, headers);
};

/**
 * PUT the file at `src` to `filename`, with callback `fn`
 * receiving a possible exception, and the response object.
 *
 * NOTE: this method reads the _entire_ file into memory using
 * fs.readFile(), and is not recommended or large files.
 *
 * Example:
 *
 *    client
 *     .putFile('package.json', '/test/package.json', function(err, res){
 *       if (err) throw err;
 *       console.log(res.statusCode);
 *       console.log(res.headers);
 *     });
 *
 * @param {String} src
 * @param {String} filename
 * @param {Object|Function} headers
 * @param {Function} fn
 * @api public
 */

Client.prototype.putFile = function(src, filename, headers, fn){
  var self = this;
  if ('function' == typeof headers) {
    fn = headers;
    headers = {};
  };
  fs.readFile(src, function(err, buf){
    if (err) return fn(err);
    headers = utils.merge({
        'Content-Length': buf.length
      , 'Content-Type': mime.lookup(src)
      , 'Content-MD5': crypto.createHash('md5').update(buf).digest('base64')
    }, headers);
    self.put(filename, headers).on('response', function(res){
      fn(null, res);
    }).end(buf);
  });
};

/**
 * PUT the file as `src` to `filename`, with callback `fn`
 * receiving a possible exception, and the response object.
 */
Client.prototype.putMultipartFile = function(src, filename, headers, fn) {
    var self = this;
    if('function' == typeof headers) {
        fn = headers;
        headers = {};
    }
    fs.stat(src, function(err, stats) {
        if (err) return fn(err);
        
        var MINIMUM_SIZE = 5 * 1024 * 1024;     // S3 requires each part be at least 5MB
        var MAX_PARTS = 10000;                  // S3 only allows up to 10,000 parts
        var filesize = stats.size;
        
        if(filesize < MINIMUM_SIZE) {
            // In the case that the file is too small, do a normal S3 upload
            self.putFile(src, filename, headers, fn);
        } else {
            // Calculate how many chunks / chunk size
            var num_chunks = MAX_PARTS;
            var chunk_size = MINIMUM_SIZE;
            if(filesize > MAX_PARTS * MINIMUM_SIZE) {
                // If the filesize is greater than 48.828125 gigs, we have to use chunks bigger than 5MB
                chunk_size = Math.ceil(filesize * 1.0 / MAX_PARTS);
            } else {
                num_chunks = Math.ceil(filesize * 1.0 / MINIMUM_SIZE);
            }

            fs.open(src, 'r', function(err, fd) {
                if(err) return fn(err);
                
                var finishUpload = utils.after(num_chunks, function(uploadId, eTagRegistry){
                    content = "<CompleteMultipartUpload>";
                    for(var i=1;i<eTagRegistry.length;i++) {
                        content += "<Part>";
                        content += "<PartNumber>" + i + "</PartNumber>";
                        content += "<ETag>" + eTagRegistry[i] + "</ETag>";
                        content += "</Part>";
                    }
                    content += "</CompleteMultipartUpload>";
                    
                    buff = new Buffer(content);
                    headers = {
                        'Content-Length': buff.length
                    }
                    var req = self.request("POST", filename + "?uploadId=" + uploadId, headers);
                    req.on('response', function(res) {
                        res.setEncoding('utf8');
                        var completeUploadResponse = "";
                        res.on('data', function(chunk) {
                            completeUploadResponse += chunk;
                        });
                        res.on('end', function() {
                            fn(null, completeUploadResponse);
                        });
                    });
                    req.end(buff);                    
                });
                
                headers = utils.merge({
                    'Content-Type': mime.lookup(src)
                }, headers);
                var req = self.request("POST", filename + "?uploads", headers);
                
                req.on('response', function(res){
                    var xmlResponse = "";
                    res.setEncoding('utf8');
                    res.on('data', function(chunk){
                        xmlResponse+=chunk;
                    });
                    res.on('end', function() {
                        // Get the uploadId from the XML response
                        // Node doesn't have any built in XML processing so do it using some dirty
                        // string magic.
                        var start = xmlResponse.indexOf("<UploadId>") + "<UploadId>".length;
                        var end = xmlResponse.indexOf("</UploadId>");
                        var uploadId = xmlResponse.substr(start, end-start);
                                                
                        var eTagRegistry = [];      // store all of the partNum / Etag combinations to finish the upload
                        
                        var uploadPart = function(partNum) {
                            var buff = new Buffer(chunk_size);
                            fs.read(fd, buff, 0, chunk_size, (i-1)*chunk_size, function(err, bytesRead, buffer) {
                                var dataToUpload = buffer.slice(0, bytesRead);
                                
                                headers = {
                                    'Content-Length': bytesRead,
                                    'Content-MD5': crypto.createHash('md5').update(dataToUpload).digest('base64')
                                };
                                
                                var resource = filename + "?partNumber=" + partNum + "&uploadId=" + uploadId;
                                var uploadRequest = self.request("PUT", resource, headers);
                                uploadRequest.on('response', function(response) {
                                    if(response.statusCode != 200) {
                                        // How do we want to handle errors for individual parts of the upload?
                                        // Retry a certain number of times? Before failing totally?
                                        console.log("SOMETHING BAD HAPPENED!");
                                    } else {
                                        // save the eTag in the registry and pass it along to finishUpload
                                        // by the time finishUpload is actually executed the registry should be full.
                                        eTagRegistry[parseInt(partNum)] = response.headers.etag;
                                        finishUpload(uploadId, eTagRegistry);                                        
                                    }
                                });
                                uploadRequest.end(dataToUpload, 'binary');
                            });
                        }
                        // Upload 'num_chunk' parts of size 'chunk_size'
                        for(var i=1;i<=num_chunks;i++) {
                            uploadPart(i);
                        } 
                    });                    
                });
                req.end();       
            });            
        }
    });
};

/**
 * PUT the given `stream` as `filename` with optional `headers`.
 *
 * @param {Stream} stream
 * @param {String} filename
 * @param {Object|Function} headers
 * @param {Function} fn
 * @api public
 */

Client.prototype.putStream = function(stream, filename, headers, fn){
  var self = this;
  if ('function' == typeof headers) {
    fn = headers;
    headers = {};
  };
  fs.stat(stream.path, function(err, stat){
    if (err) return fn(err);
    // TODO: sys.pump() wtf?
    var req = self.put(filename, utils.merge({
        'Content-Length': stat.size
      , 'Content-Type': mime.lookup(stream.path)
    }, headers));
    req.on('response', function(res){
      fn(null, res);
    });
    stream
      .on('error', function(err){fn(null, err); })
      .on('data', function(chunk){ req.write(chunk); })
      .on('end', function(){ req.end(); });
  });
};

/**
 * GET `filename` with optional `headers`.
 *
 * @param {String} filename
 * @param {Object} headers
 * @return {ClientRequest}
 * @api public
 */

Client.prototype.get = function(filename, headers){
  return this.request('GET', filename, headers);
};

/**
 * GET `filename` with optional `headers` and callback `fn`
 * with a possible exception and the response.
 *
 * @param {String} filename
 * @param {Object|Function} headers
 * @param {Function} fn
 * @api public
 */

Client.prototype.getFile = function(filename, headers, fn){
  if ('function' == typeof headers) {
    fn = headers;
    headers = {};
  }
  return this.get(filename, headers).on('response', function(res){
    fn(null, res);
  }).end();
};

/**
 * Issue a HEAD request on `filename` with optional `headers.
 *
 * @param {String} filename
 * @param {Object} headers
 * @return {ClientRequest}
 * @api public
 */

Client.prototype.head = function(filename, headers){
  return this.request('HEAD', filename, headers);
};

/**
 * Issue a HEAD request on `filename` with optional `headers`
 * and callback `fn` with a possible exception and the response.
 *
 * @param {String} filename
 * @param {Object|Function} headers
 * @param {Function} fn
 * @api public
 */

Client.prototype.headFile = function(filename, headers, fn){
  if ('function' == typeof headers) {
    fn = headers;
    headers = {};
  }
  return this.head(filename, headers).on('response', function(res){
    fn(null, res);
  }).end();
};

/**
 * DELETE `filename` with optional `headers.
 *
 * @param {String} filename
 * @param {Object} headers
 * @return {ClientRequest}
 * @api public
 */

Client.prototype.del = function(filename, headers){
  return this.request('DELETE', filename, headers);
};

/**
 * DELETE `filename` with optional `headers`
 * and callback `fn` with a possible exception and the response.
 *
 * @param {String} filename
 * @param {Object|Function} headers
 * @param {Function} fn
 * @api public
 */

Client.prototype.deleteFile = function(filename, headers, fn){
  if ('function' == typeof headers) {
    fn = headers;
    headers = {};
  }
  return this.del(filename, headers).on('response', function(res){
    fn(null, res);
  }).end();
};

/**
 * Return a url to the given `filename`.
 *
 * @param {String} filename
 * @return {String}
 * @api public
 */

Client.prototype.url =
Client.prototype.http = function(filename){
  return 'http://' + join(this.endpoint, this.bucket, filename);
};

/**
 * Return an HTTPS url to the given `filename`.
 *
 * @param {String} filename
 * @return {String}
 * @api public
 */

Client.prototype.https = function(filename){
  return 'https://' + join(this.endpoint, filename);
};

/**
 * Return an S3 presigned url to the given `filename`.
 *
 * @param {String} filename
 * @param {Date} expiration
 * @return {String}
 * @api public
 */

Client.prototype.signedUrl = function(filename, expiration){
  var epoch = Math.floor(expiration.getTime()/1000);
  var signature = auth.signQuery({
    secret: this.secret,
    date: epoch,
    resource: '/' + this.bucket + url.parse(filename).pathname
  });

  return this.url(filename) +
    '?Expires=' + epoch +
    '&AWSAccessKeyId=' + this.key +
    '&Signature=' + encodeURIComponent(signature);
};

/**
 * Shortcut for `new Client()`.
 *
 * @param {Object} options
 * @see Client()
 * @api public
 */

exports.createClient = function(options){
  return new Client(options);
};
