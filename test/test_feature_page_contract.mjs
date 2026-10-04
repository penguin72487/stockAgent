import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

const context = vm.createContext({});
for (const file of ['feature-page-constraints.js','feature-page-contract.js']) {
  vm.runInContext(readFileSync(new URL(`../services/public_dashboards/${file}`,import.meta.url),'utf8'),context);
}
const verify = context.StockAgentFeaturePage.validateFeaturePage;
const page = () => ({schema_version:1,read_only:true,production_control_possible:false,
  generated_at_utc:null,revision:'a'.repeat(32),reset_required:false,
  offset:0,limit:2,matching_total:3,has_more:true,summary:{},filters:{},
  rows:[{code:'0050',metadata_count:2**128},{code:'2330'}]});

test('typed response validation preserves original rows, leading zeros and unrestricted row metadata', () => {
  const value=page();const result=verify(value,0,2);
  assert.equal(result,value);assert.equal(result.rows,value.rows);
  assert.equal(result.rows[0].code,'0050');assert.equal(result.rows[0].metadata_count,2**128);
});

test('invalid safety, schema, revision, row coverage and pagination never enter the typed client', () => {
  for (const patch of [{schema_version:'1'},{schema_version:2},{read_only:false},
    {production_control_possible:true},{revision:'bad'},{generated_at_utc:12},
    {rows:[null,null]},{filters:[]},{summary:null},{has_more:false},{offset:2},
    {limit:3},{rows:[]},{matching_total:Number.MAX_SAFE_INTEGER+1},
    {reset_required:'true'}]) assert.throws(()=>verify({...page(),...patch},0,2));
  for (const input of [null,undefined,[],false]) assert.throws(()=>verify(input,0,2));
});

test('generation reset accepts only page zero and validates the requested limit', () => {
  const value={...page(),reset_required:true};assert.equal(verify(value,2,2),value);
  assert.throws(()=>verify({...value,offset:2},2,2));
  assert.throws(()=>verify(value,-1,2));assert.throws(()=>verify(value,0,5001));
});
