// @flow

'use strict';
const zlib = require('zlib');
const snappy = require('snappyjs');
const lzo = require('lzo');
const brotli = require('brotli');

const PARQUET_COMPRESSION_METHODS = {
  'UNCOMPRESSED': {
    deflate: deflate_identity,
    inflate: inflate_identity
  },
  'GZIP': {
    deflate: deflate_gzip,
    inflate: inflate_gzip
  },
  'SNAPPY': {
    deflate: deflate_snappy,
    inflate: inflate_snappy
  },
  'LZO': {
    deflate: deflate_lzo,
    inflate: inflate_lzo
  },
  'BROTLI': {
    deflate: deflate_brotli,
    inflate: inflate_brotli
  }
};

type Method = 'UNCOMPRESSED' | 'GZIP' | 'SNAPPY' | 'LZO' | 'BROTLI';

/**
 * Deflate a value using compression method `method`
 */
function deflate(method: Method, value: Buffer) {
  if (!(method in PARQUET_COMPRESSION_METHODS)) {
    throw 'invalid compression method: ' + method;
  }

  return PARQUET_COMPRESSION_METHODS[method].deflate(value);
}

function deflate_identity(value: Buffer) {
  return value;
}

function deflate_gzip(value: Buffer) {
  return zlib.gzipSync(value);
}

function deflate_snappy(value: Buffer) {
  return snappy.compress(value);
}

function deflate_lzo(value: Buffer) {
  return lzo.compress(value);
}

function deflate_brotli(value: Buffer) {
  return new Buffer(brotli.compress(value, {
    mode: 0,
    quality: 8,
    lgwin: 22
  }));
}

/**
 * Inflate a value using compression method `method`
 */
function inflate(method: Method, value: Buffer) {
  if (!(method in PARQUET_COMPRESSION_METHODS)) {
    throw 'invalid compression method: ' + method;
  }

  return PARQUET_COMPRESSION_METHODS[method].inflate(value);
}

function inflate_identity(value: Buffer) {
  return value;
}

function inflate_gzip(value: Buffer) {
  return zlib.gunzipSync(value);
}

function inflate_snappy(value: Buffer) {
  return snappy.uncompress(value);
}

function inflate_lzo(value: Buffer) {
  return lzo.decompress(value);
}

function inflate_brotli(value: Buffer) {
  return new Buffer(brotli.decompress(value));
}

module.exports = { PARQUET_COMPRESSION_METHODS, deflate, inflate };

