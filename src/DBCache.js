"use strict";

var _ = require("lodash");
var redis = require('redis');
var async = require('async');

var log = require("./logUtil");

var commonService = require("./dbServices");
//declare global cache
global.lastSyncTime = 0;


var COMPARE_FIELDS = ["id", "gia", "loaiTin", "dienTich", "soPhongNgu", "soTang", "soPhongTam", "image"
  , "place", "loaiNhaDat", "huongNha", "ngayDangTin", "chiTiet", "dangBoi"];

var FIELDS =
  [ "id"
  , "gia", "loaiTin", "dienTich", "soPhongNgu", "soTang", "soPhongTam"
  , "place.diaChinh.codeTinh", "place.diaChinh.codeHuyen", "place.diaChinh.codeXa", "place.diaChinh.codeDuAn"
  , "place.geo.lat", "place.geo.lon"
  , "giaM2", "loaiNhaDat", "huongNha", "ngayDangTin", "chiTiet", "timeExtracted"
  , "dangBoi.name", "dangBoi.phone", "dangBoi.email"
  , "`image.cover`", "image.images"
  ];

function loadAds(redisClient, callback) {

  commonService.query("select count(*) from default where type='Ads' and timeModified >= 0", (err, list) => {
    log.info("Couchbae DB count:", list);
  });


  //let projection = "id, gia, loaiTin, dienTich, soPhongNgu, soTang, soPhongTam, image, place, giaM2, loaiNhaDat, huongNha, ngayDangTin,timeExtracted ";
  //projection = isFull ? "`timeModified`,`id`,`gia`,`loaiTin`,`dienTich`,`soPhongNgu`,`soTang`,`soPhongTam`,`image`,`place`,`giaM2`,`loaiNhaDat`,`huongNha`,`ngayDangTin`,`chiTiet`,`dangBoi`,`source`,`type`,`maSo`,`url`,`GEOvsDC`,`GEOvsDC_distance`,`GEOvsDC_radius`,`timeExtracted`" : projection;
  let projection = FIELDS.join(",");
  let sql = `select ${projection} from default where type='Ads' and timeModified >= ${global.lastSyncTime}  ` ;

  commonService.query(sql, (err, list) => {
    if (err) return console.error(err);

    log.info("Number of records from DB:" + list.length);

    let processOne = (e, doneOne) => {
      let ads = _.cloneDeep(e);
      delete ads.images;
      deleteNull(ads);

      let fl = [];
      fl.push((callback) => {
        redisClient.HMSET(ads.id
          , ads
          , (err, rep) => {
            if (err) {log.error("error:", err, rep);}
            callback();
          });
      });

      let addIndex = (field, callback) => {
        if (ads[field] === null || ads[field] === undefined) {
          return callback();
        }

        redisClient.SADD(field + ":" + ads[field] , ads.id
          , (err, rep) => {
            if (err) log.error("error:", err, rep);
            //console.log("SADD = ", field + ":" + ads[field], ads.id);
            callback();
          });
      };

      let addZIndex = (field, callback) => {
        if (!ads[field]) {
          return callback();
        }

        redisClient.ZADD(field, ads[field] , ads.id
          , (err, rep) => {
            if (err) log.error("error:", err, rep);
            callback();
          });
      };

      fl.push((callback) => addIndex("loaiTin", callback));
      fl.push((callback) => addIndex("loaiNhaDat", callback));
      fl.push((callback) => addIndex("soPhongNgu", callback));
      fl.push((callback) => addIndex("soPhongTam", callback));
      fl.push((callback) => addIndex("soTang", callback));
      fl.push((callback) => addIndex("huongNha", callback));

      fl.push((callback) => addIndex("codeTinh", callback));
      fl.push((callback) => addIndex("codeHuyen", callback));
      fl.push((callback) => addIndex("codeXa", callback));
      fl.push((callback) => addIndex("codeDuAn", callback));
      fl.push((callback) => addIndex("ngayDangTin", callback));

      fl.push((callback) => addZIndex("gia", callback));
      fl.push((callback) => addZIndex("dienTich", callback));
      fl.push((callback) => addZIndex("lat", callback));
      fl.push((callback) => addZIndex("lon", callback));


      async.parallel(fl, () => {
        //console.log("Done One");
        doneOne();
      })

    };

    async.eachLimit(list, 100, processOne , (err1) => {

      if (err1) log.error("Error when prcess all to Redis...", err1);

      console.log("Done load all Ads ", list.length + " records");

      callback(list.length);
    });

  });
}

function deleteNull(e) {
  for (let a in e) {
    if (e[a] == null || e[a] == undefined) {
      delete e[a];
    }
  }
}

var cursor = '0';

function scan(redisClient){
  redisClient.scan(cursor, 'MATCH', 'Ads_0_*', 'COUNT', '5', function(err, reply){
    if(err){
      throw err;
    }
    cursor = reply[0];
    if(cursor === '0'){
      return console.log('Scan Complete');
    }else{
      // do your processing
      // reply[1] is an array of matched keys.
       console.log("Found:", reply[1]);
      //return scan();
    }
  });
}


var cache = {
  updateLastSyncTime(lastSyncTime) {
    global.lastSyncTime = lastSyncTime || new Date().getTime();
  },

  _loadingAds : false,

  reloadAds(done) {
    if (this._loadingAds) {
      log.error("Can't perform reloadAds, there is readAds running!");
      return;
    }
    this._loadingAds = true;
    let that = this;

    let total = 0;

    var redisClient = redis.createClient({host : 'localhost', port : 6379});

    redisClient.on('ready',function() {
      console.log("Redis is ready!");

      loadAds(redisClient, (length)=> {
        total += length;
        redisClient.dbsize((err, rep) => {
          log.info("Get size:" , rep);
        });

        console.log("Total loaded ads : " + total + ", from loki ads:" );
        that._loadingAds = false;

        //
        //scan(redisClient);

        done && done();
      });
    });

    redisClient.on('error',function() {
      console.log("Error in Redis");
    });
  },
  adsRawAsMap() {
    return global.rwcache.Ads_Raw.asMap;
  },

  adsRawAsArray() {
    return global.rwcache.Ads_Raw.asArray;
  },

  adsById(id) {
    return adsCol.by('id', id);
  },

  query(q, callback){
    let startQuery = new Date().getTime();

    if (q.huongNha && q.huongNha.length==1 && q.huongNha[0] == 0) {
      q.huongNha = null;
    }
    //sorting
    let orderBy = q.orderBy || {"name": "ngayDangTin", "type":"DESC"};

    let that = this;

    let filtered = [];
    filtered = adsCol.chain()
      .find({loaiTin:q.loaiTin})
      .where((e) => {
        return that._match(q, e)
      })
      .data();

    //ordering
    let count = filtered.length;

    console.log("Filterred length: ", count);

    let sign = 1;
    if (orderBy.type == 'DESC') {
      sign = -1;
    }
    console.log("Will sort by ", orderBy, sign);

    let startTime = new Date().getTime();
    filtered.sort((a, b) => {
      if (a[orderBy.name] > b[orderBy.name]) {
        return sign;
      }

      if (a[orderBy.name] < b[orderBy.name]) {
        return -1 * sign;
      }

      if (a.timeExtracted > b.timeExtracted) {
        return sign;
      }
      if (a.timeExtracted < b.timeExtracted) {
        return -1 * sign;
      }

      if (a.id > b.id) {
        return sign;
      }

      if (a.id < b.id) {
        return -1 * sign;
      }

      return 0;
    });
    let endTime = new Date().getTime();

    console.log("Sorting time " + (endTime - startTime) + " ms for " + filtered.length + " records");

    //do paging
    filtered = filtered.slice((q.dbPageNo-1)*q.dbLimit, q.dbPageNo*q.dbLimit);

    let endQuery = new Date().getTime();

    console.log("Query time " + (endQuery - startQuery) + " ms for " + filtered.length + " records");

    callback(null, filtered, count);

    return filtered;
  },

  _match(q, ads){
    if (q.loaiTin !== ads.loaiTin) {
      return false;
    }

    if(q.loaiNhaDat && _.indexOf(q.loaiNhaDat, ads.loaiNhaDat) === -1){
      //logUtil.info("Not match loaiNhaDat", q.loaiNhaDat, ads.loaiNhaDat);
      return false;
    }

    if (q.viewport) {
      let vp = q.viewport;
      let geo = ads.place.geo;

      if (geo.lat < vp.southwest.lat || geo.lat > vp.northeast.lat
        || geo.lon < vp.southwest.lon || geo.lon > vp.northeast.lon
      ) {
        //logUtil.info("Not match viewport", vp, geo);
        return false;
      }
    }

    if (q.diaChinh) {
      let dc = q.diaChinh;
      if (dc.tinhKhongDau && ads.place.diaChinh.codeTinh !== dc.tinhKhongDau) {
        //logUtil.info("Not match codeTinh", dc.tinhKhongDau, ads.place.diaChinh.codeTinh);
        return false;
      }

      if (dc.huyenKhongDau && ads.place.diaChinh.codeHuyen !== dc.huyenKhongDau) {
        //logUtil.info("Not match codeHuyen", dc.huyenKhongDau, ads.place.diaChinh.codeHuyen);
        return false;
      }
      if (dc.xaKhongDau && ads.place.diaChinh.codeXa !== dc.xaKhongDau) {
        return false;
      }
      if (dc.duAnKhongDau && ads.place.diaChinh.codeDuAn !== dc.duAnKhongDau) {
        return false;
      }
    }

    if (q.ngayDangTinGREATER && ads.ngayDangTin <= q.ngayDangTinGREATER) { //ngayDangTinFrom: 20-04-2016
      return false;
    }

    if (q.giaBETWEEN && (q.giaBETWEEN[0] > 0 || q.giaBETWEEN[1] < 9999999)) {
      if (ads.gia < q.giaBETWEEN[0] || ads.gia > q.giaBETWEEN[1]) {
        return false;
      }
    }

    if(q.soPhongNguGREATER){
      let soPhongNguGREATER = Number(q.soPhongNguGREATER);
      if (soPhongNguGREATER && ads.soPhongNgu < soPhongNguGREATER) {
        return false;
      }
    }


    if(q.soPhongTamGREATER){
      let soPhongTamGREATER = Number(q.soPhongTamGREATER);
      if (soPhongTamGREATER && ads.soPhongTam < soPhongTamGREATER) {
        return false;
      }
    }
    if(q.soTangGREATER){
      let soTangGREATER = Number(q.soTangGREATER);
      if (soTangGREATER && ads.soTang < soTangGREATER) {
        return false;
      }
    }

    if ((q.dienTichBETWEEN) && (q.dienTichBETWEEN[0] > 0 || q.dienTichBETWEEN[1] < 9999999)) {
      if (ads.dienTich < q.dienTichBETWEEN[0] || ads.dienTich > q.dienTichBETWEEN[1]) {
        return false;
      }
    }

    if(q.huongNha  && _.indexOf(q.huongNha, ads.huongNha) === -1){
      return false;
    }

    if(q.soPhongNgu){
      let soPhongNgu = Number(q.soPhongNgu);
      if (ads.soPhongNgu !== soPhongNgu) {
        return false;
      }
    }
    if(q.soPhongTam){
      let soPhongTam = Number(q.soPhongTam);
      if (ads.soPhongTam !== soPhongTam) {
        return false;
      }
    }

    if(q.soTang){
      let soTang = Number(q.soTang);
      if (ads.soTang !== soTang) {
        return false;
      }
    }

    if (q.gia) {
      if (ads.gia !== q.gia) {
        return false;
      }
    }

    if (q.dienTich) {
      if (ads.dienTich !== q.dienTich) {
        return false;
      }
    }

    return true;
  }

};

module.exports = cache;