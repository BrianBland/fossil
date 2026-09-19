var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// build/worker/shim.mjs
import Pe from "./c78c7582b5397cd6f4bd597e51113c775473978a-index.wasm";
import { WorkerEntrypoint as Je } from "cloudflare:workers";
var X = Object.defineProperty;
var N = /* @__PURE__ */ __name((e, t) => {
  for (var n in t) X(e, n, { get: t[n], enumerable: true });
}, "N");
var m = {};
N(m, { ContainerStartupOptions: /* @__PURE__ */ __name(() => R, "ContainerStartupOptions"), IntoUnderlyingByteSource: /* @__PURE__ */ __name(() => k, "IntoUnderlyingByteSource"), IntoUnderlyingSink: /* @__PURE__ */ __name(() => A, "IntoUnderlyingSink"), IntoUnderlyingSource: /* @__PURE__ */ __name(() => z, "IntoUnderlyingSource"), MinifyConfig: /* @__PURE__ */ __name(() => O, "MinifyConfig"), R2Range: /* @__PURE__ */ __name(() => M, "R2Range"), __wbg_String_8564e559799eccda: /* @__PURE__ */ __name(() => Q, "__wbg_String_8564e559799eccda"), __wbg___wbindgen_debug_string_0e68cf47c9cbd9b0: /* @__PURE__ */ __name(() => Z, "__wbg___wbindgen_debug_string_0e68cf47c9cbd9b0"), __wbg___wbindgen_in_50072d4d6e45c193: /* @__PURE__ */ __name(() => tt, "__wbg___wbindgen_in_50072d4d6e45c193"), __wbg___wbindgen_is_function_fcda5e3902d732fe: /* @__PURE__ */ __name(() => et, "__wbg___wbindgen_is_function_fcda5e3902d732fe"), __wbg___wbindgen_is_null_5160b3e381865372: /* @__PURE__ */ __name(() => nt, "__wbg___wbindgen_is_null_5160b3e381865372"), __wbg___wbindgen_is_null_or_undefined_4b0bf0653367120c: /* @__PURE__ */ __name(() => rt, "__wbg___wbindgen_is_null_or_undefined_4b0bf0653367120c"), __wbg___wbindgen_is_undefined_8c687d0b90d5b524: /* @__PURE__ */ __name(() => _t, "__wbg___wbindgen_is_undefined_8c687d0b90d5b524"), __wbg___wbindgen_reinit_0256f5d04898665d: /* @__PURE__ */ __name(() => it, "__wbg___wbindgen_reinit_0256f5d04898665d"), __wbg___wbindgen_string_get_92ab86bb19cbc12f: /* @__PURE__ */ __name(() => ot, "__wbg___wbindgen_string_get_92ab86bb19cbc12f"), __wbg___wbindgen_throw_5d9e815e6fdf150f: /* @__PURE__ */ __name(() => st, "__wbg___wbindgen_throw_5d9e815e6fdf150f"), __wbg__wbg_cb_unref_997e73d32238e655: /* @__PURE__ */ __name(() => ct, "__wbg__wbg_cb_unref_997e73d32238e655"), __wbg_bodyUsed_930a158a8e1aec0f: /* @__PURE__ */ __name(() => ut, "__wbg_bodyUsed_930a158a8e1aec0f"), __wbg_body_60100aca0566fdf4: /* @__PURE__ */ __name(() => ft, "__wbg_body_60100aca0566fdf4"), __wbg_body_98d7808e69835fb7: /* @__PURE__ */ __name(() => at, "__wbg_body_98d7808e69835fb7"), __wbg_buffer_4a989bded7035f57: /* @__PURE__ */ __name(() => bt, "__wbg_buffer_4a989bded7035f57"), __wbg_byobRequest_f161fc37241dd3d4: /* @__PURE__ */ __name(() => dt, "__wbg_byobRequest_f161fc37241dd3d4"), __wbg_byteLength_0ddb1795e2c7e689: /* @__PURE__ */ __name(() => gt, "__wbg_byteLength_0ddb1795e2c7e689"), __wbg_byteOffset_46eb015f52b6ad7d: /* @__PURE__ */ __name(() => wt, "__wbg_byteOffset_46eb015f52b6ad7d"), __wbg_call_6bcf8d3e20937e46: /* @__PURE__ */ __name(() => pt, "__wbg_call_6bcf8d3e20937e46"), __wbg_cancel_c076350a7e8954be: /* @__PURE__ */ __name(() => lt, "__wbg_cancel_c076350a7e8954be"), __wbg_catch_e2134ef6dcdb69ed: /* @__PURE__ */ __name(() => ht, "__wbg_catch_e2134ef6dcdb69ed"), __wbg_cause_36bd622854f9b3a4: /* @__PURE__ */ __name(() => yt, "__wbg_cause_36bd622854f9b3a4"), __wbg_cf_c7b0f56b1787c078: /* @__PURE__ */ __name(() => mt, "__wbg_cf_c7b0f56b1787c078"), __wbg_close_22882088c136df25: /* @__PURE__ */ __name(() => xt, "__wbg_close_22882088c136df25"), __wbg_close_8b609460dd26e367: /* @__PURE__ */ __name(() => It, "__wbg_close_8b609460dd26e367"), __wbg_constructor_207e5664c25e1af9: /* @__PURE__ */ __name(() => vt, "__wbg_constructor_207e5664c25e1af9"), __wbg_enqueue_6cb545d22db14f33: /* @__PURE__ */ __name(() => Et, "__wbg_enqueue_6cb545d22db14f33"), __wbg_error_756c5934221e6fee: /* @__PURE__ */ __name(() => Ft, "__wbg_error_756c5934221e6fee"), __wbg_error_b7e73e671a58a488: /* @__PURE__ */ __name(() => jt, "__wbg_error_b7e73e671a58a488"), __wbg_getReader_bb0230851fbf986b: /* @__PURE__ */ __name(() => Wt, "__wbg_getReader_bb0230851fbf986b"), __wbg_get_423ea1f7ff5c8d64: /* @__PURE__ */ __name(() => St, "__wbg_get_423ea1f7ff5c8d64"), __wbg_get_94d18d21679927f6: /* @__PURE__ */ __name(() => Rt, "__wbg_get_94d18d21679927f6"), __wbg_get_989d0a1309644f2b: /* @__PURE__ */ __name(() => kt, "__wbg_get_989d0a1309644f2b"), __wbg_get_b5793eadbdf6b0ae: /* @__PURE__ */ __name(() => At, "__wbg_get_b5793eadbdf6b0ae"), __wbg_get_done_a668aa62d81fad70: /* @__PURE__ */ __name(() => zt, "__wbg_get_done_a668aa62d81fad70"), __wbg_get_value_6c62a77c168d825a: /* @__PURE__ */ __name(() => Ot, "__wbg_get_value_6c62a77c168d825a"), __wbg_head_70d4ddc719656838: /* @__PURE__ */ __name(() => Mt, "__wbg_head_70d4ddc719656838"), __wbg_headers_a1e9854406915ee7: /* @__PURE__ */ __name(() => Tt, "__wbg_headers_a1e9854406915ee7"), __wbg_httpEtag_3ae37bea99ffb11e: /* @__PURE__ */ __name(() => Ut, "__wbg_httpEtag_3ae37bea99ffb11e"), __wbg_instanceId_232fcea31f0d9b32: /* @__PURE__ */ __name(() => Lt, "__wbg_instanceId_232fcea31f0d9b32"), __wbg_instanceof_Error_fe6fa771c78ee4cf: /* @__PURE__ */ __name(() => qt, "__wbg_instanceof_Error_fe6fa771c78ee4cf"), __wbg_instanceof_ReadableStream_07dda5955244a495: /* @__PURE__ */ __name(() => Dt, "__wbg_instanceof_ReadableStream_07dda5955244a495"), __wbg_length_31bdaf014f5fbde2: /* @__PURE__ */ __name(() => Ct, "__wbg_length_31bdaf014f5fbde2"), __wbg_message_1cbc5bc03dcf1dee: /* @__PURE__ */ __name(() => Bt, "__wbg_message_1cbc5bc03dcf1dee"), __wbg_method_f625716bf1f7b540: /* @__PURE__ */ __name(() => $t, "__wbg_method_f625716bf1f7b540"), __wbg_name_b4e1ee96e711fbdf: /* @__PURE__ */ __name(() => Nt, "__wbg_name_b4e1ee96e711fbdf"), __wbg_name_b9d8f2ea16b22045: /* @__PURE__ */ __name(() => Vt, "__wbg_name_b9d8f2ea16b22045"), __wbg_new_0afe64b4dc16ab74: /* @__PURE__ */ __name(() => Ht, "__wbg_new_0afe64b4dc16ab74"), __wbg_new_a32a1ab6c6655abe: /* @__PURE__ */ __name(() => Pt, "__wbg_new_a32a1ab6c6655abe"), __wbg_new_bebc3f4757acf305: /* @__PURE__ */ __name(() => Jt, "__wbg_new_bebc3f4757acf305"), __wbg_new_typed_6f8b0d724fe26c07: /* @__PURE__ */ __name(() => Xt, "__wbg_new_typed_6f8b0d724fe26c07"), __wbg_new_with_byte_offset_and_length_492c969e8b5da8a4: /* @__PURE__ */ __name(() => Gt, "__wbg_new_with_byte_offset_and_length_492c969e8b5da8a4"), __wbg_new_with_length_5ffeddb9d9fbb96f: /* @__PURE__ */ __name(() => Yt, "__wbg_new_with_length_5ffeddb9d9fbb96f"), __wbg_new_with_opt_buffer_source_and_init_935ee9753c594708: /* @__PURE__ */ __name(() => Kt, "__wbg_new_with_opt_buffer_source_and_init_935ee9753c594708"), __wbg_new_with_opt_readable_stream_and_init_652b1d60607d75aa: /* @__PURE__ */ __name(() => Qt, "__wbg_new_with_opt_readable_stream_and_init_652b1d60607d75aa"), __wbg_new_with_opt_str_and_init_cbaf7a52e7dc0b00: /* @__PURE__ */ __name(() => Zt, "__wbg_new_with_opt_str_and_init_cbaf7a52e7dc0b00"), __wbg_prototypesetcall_ae9f5e7459250748: /* @__PURE__ */ __name(() => te, "__wbg_prototypesetcall_ae9f5e7459250748"), __wbg_queueMicrotask_85c90f6987555d65: /* @__PURE__ */ __name(() => ee, "__wbg_queueMicrotask_85c90f6987555d65"), __wbg_queueMicrotask_f6a1fa10b81d1fc0: /* @__PURE__ */ __name(() => ne, "__wbg_queueMicrotask_f6a1fa10b81d1fc0"), __wbg_read_31091533ffadf971: /* @__PURE__ */ __name(() => re, "__wbg_read_31091533ffadf971"), __wbg_releaseLock_1538945f5f183d9f: /* @__PURE__ */ __name(() => _e, "__wbg_releaseLock_1538945f5f183d9f"), __wbg_resolve_35ec7e0c6af4c82c: /* @__PURE__ */ __name(() => ie, "__wbg_resolve_35ec7e0c6af4c82c"), __wbg_respond_83a71686e927ca32: /* @__PURE__ */ __name(() => oe, "__wbg_respond_83a71686e927ca32"), __wbg_set_5f2ad37e5e02dc7b: /* @__PURE__ */ __name(() => se, "__wbg_set_5f2ad37e5e02dc7b"), __wbg_set_7923e5ea63b41e6b: /* @__PURE__ */ __name(() => ce, "__wbg_set_7923e5ea63b41e6b"), __wbg_set_a377297433dfea63: /* @__PURE__ */ __name(() => ue, "__wbg_set_a377297433dfea63"), __wbg_set_criticalError_e391f6c6a38c6d68: /* @__PURE__ */ __name(() => fe, "__wbg_set_criticalError_e391f6c6a38c6d68"), __wbg_set_headers_dfe6a763facd3dcc: /* @__PURE__ */ __name(() => ae, "__wbg_set_headers_dfe6a763facd3dcc"), __wbg_set_instanceId_b0493682e07f9aa4: /* @__PURE__ */ __name(() => be, "__wbg_set_instanceId_b0493682e07f9aa4"), __wbg_set_status_74351f5228412c33: /* @__PURE__ */ __name(() => de, "__wbg_set_status_74351f5228412c33"), __wbg_set_wasm: /* @__PURE__ */ __name(() => $, "__wbg_set_wasm"), __wbg_size_9136fb55c85bae0e: /* @__PURE__ */ __name(() => ge, "__wbg_size_9136fb55c85bae0e"), __wbg_static_accessor_GLOBAL_8eb4cd83130a11a0: /* @__PURE__ */ __name(() => we, "__wbg_static_accessor_GLOBAL_8eb4cd83130a11a0"), __wbg_static_accessor_GLOBAL_THIS_1e7044f654e934db: /* @__PURE__ */ __name(() => pe, "__wbg_static_accessor_GLOBAL_THIS_1e7044f654e934db"), __wbg_static_accessor_INIT_STATE_86fe3c7381036fc1: /* @__PURE__ */ __name(() => le, "__wbg_static_accessor_INIT_STATE_86fe3c7381036fc1"), __wbg_static_accessor_SELF_d8b50611246a6d92: /* @__PURE__ */ __name(() => he, "__wbg_static_accessor_SELF_d8b50611246a6d92"), __wbg_static_accessor_WINDOW_fd0bc376bf0f8b42: /* @__PURE__ */ __name(() => ye, "__wbg_static_accessor_WINDOW_fd0bc376bf0f8b42"), __wbg_then_7a850dae4493f353: /* @__PURE__ */ __name(() => me, "__wbg_then_7a850dae4493f353"), __wbg_then_b830475380919203: /* @__PURE__ */ __name(() => xe, "__wbg_then_b830475380919203"), __wbg_url_8d180a4a6996e22d: /* @__PURE__ */ __name(() => Ie, "__wbg_url_8d180a4a6996e22d"), __wbg_view_d8c7b26e4d4650f1: /* @__PURE__ */ __name(() => ve, "__wbg_view_d8c7b26e4d4650f1"), __wbg_writeHttpMetadata_876c85f049293fec: /* @__PURE__ */ __name(() => Ee, "__wbg_writeHttpMetadata_876c85f049293fec"), __wbindgen_generic_0000000000000001: /* @__PURE__ */ __name(() => Fe, "__wbindgen_generic_0000000000000001"), __wbindgen_generic_0000000000000002: /* @__PURE__ */ __name(() => je, "__wbindgen_generic_0000000000000002"), __wbindgen_generic_0000000000000003: /* @__PURE__ */ __name(() => We, "__wbindgen_generic_0000000000000003"), __wbindgen_generic_0000000000000004: /* @__PURE__ */ __name(() => Se, "__wbindgen_generic_0000000000000004"), __wbindgen_object_clone_ref: /* @__PURE__ */ __name(() => Re, "__wbindgen_object_clone_ref"), __wbindgen_object_drop_ref: /* @__PURE__ */ __name(() => ke, "__wbindgen_object_drop_ref"), __worker_init_state: /* @__PURE__ */ __name(() => Y, "__worker_init_state"), fetch: /* @__PURE__ */ __name(() => C, "fetch"), init: /* @__PURE__ */ __name(() => K, "init") });
var q = {};
N(q, { state: /* @__PURE__ */ __name(() => L, "state") });
var L = globalThis.__worker_init_state = { criticalError: false, instanceId: 0 };
var R = class {
  static {
    __name(this, "R");
  }
  __destroy_into_raw() {
    let t = this.__wbg_ptr;
    return this.__wbg_ptr = 0, Te.unregister(this), t;
  }
  free() {
    let t = this.__destroy_into_raw();
    c(), i.__wbg_containerstartupoptions_free(t, 0);
  }
  get enableInternet() {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    let t;
    return c(), t = i.__wbg_get_containerstartupoptions_enableInternet(this.__wbg_ptr), t === 16777215 ? void 0 : t !== 0;
  }
  get entrypoint() {
    try {
      if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
      let o = i.__wbindgen_add_to_stack_pointer(-16);
      c(), i.__wbg_get_containerstartupoptions_entrypoint(o, this.__wbg_ptr);
      var t = f().getInt32(o + 0, true), n = f().getInt32(o + 4, true), r = $e(t, n);
      return i.__wbindgen_export4(t, n * 4, 4), r;
    } finally {
      i.__wbindgen_add_to_stack_pointer(16);
    }
  }
  get env() {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    let t;
    return c(), t = i.__wbg_get_containerstartupoptions_env(this.__wbg_ptr), p(t);
  }
  set enableInternet(t) {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    c(), i.__wbg_set_containerstartupoptions_enableInternet(this.__wbg_ptr, g(t) ? 16777215 : t ? 1 : 0);
  }
  set entrypoint(t) {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    let n = Ne(t, i.__wbindgen_export), r = l;
    c(), i.__wbg_set_containerstartupoptions_entrypoint(this.__wbg_ptr, n, r);
  }
  set env(t) {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    c(), i.__wbg_set_containerstartupoptions_env(this.__wbg_ptr, s(t));
  }
};
Symbol.dispose && (R.prototype[Symbol.dispose] = R.prototype.free);
var k = class {
  static {
    __name(this, "k");
  }
  __destroy_into_raw() {
    let t = this.__wbg_ptr;
    return this.__wbg_ptr = 0, Ue.unregister(this), t;
  }
  free() {
    let t = this.__destroy_into_raw();
    c(), i.__wbg_intounderlyingbytesource_free(t, 0);
  }
  get autoAllocateChunkSize() {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    let t;
    return c(), t = i.intounderlyingbytesource_autoAllocateChunkSize(this.__wbg_ptr), t >>> 0;
  }
  cancel() {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    let t = this.__destroy_into_raw();
    c(), i.intounderlyingbytesource_cancel(t);
  }
  pull(t) {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    let n;
    return c(), n = i.intounderlyingbytesource_pull(this.__wbg_ptr, s(t)), p(n);
  }
  start(t) {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    c(), i.intounderlyingbytesource_start(this.__wbg_ptr, s(t));
  }
  get type() {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    let t;
    return c(), t = i.intounderlyingbytesource_type(this.__wbg_ptr), Me[t];
  }
};
Symbol.dispose && (k.prototype[Symbol.dispose] = k.prototype.free);
var A = class {
  static {
    __name(this, "A");
  }
  __destroy_into_raw() {
    let t = this.__wbg_ptr;
    return this.__wbg_ptr = 0, Le.unregister(this), t;
  }
  free() {
    let t = this.__destroy_into_raw();
    c(), i.__wbg_intounderlyingsink_free(t, 0);
  }
  abort(t) {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    let n = this.__destroy_into_raw(), r;
    return c(), r = i.intounderlyingsink_abort(n, s(t)), p(r);
  }
  close() {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    let t = this.__destroy_into_raw(), n;
    return c(), n = i.intounderlyingsink_close(t), p(n);
  }
  write(t) {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    let n;
    return c(), n = i.intounderlyingsink_write(this.__wbg_ptr, s(t)), p(n);
  }
};
Symbol.dispose && (A.prototype[Symbol.dispose] = A.prototype.free);
var z = class {
  static {
    __name(this, "z");
  }
  __destroy_into_raw() {
    let t = this.__wbg_ptr;
    return this.__wbg_ptr = 0, qe.unregister(this), t;
  }
  free() {
    let t = this.__destroy_into_raw();
    c(), i.__wbg_intounderlyingsource_free(t, 0);
  }
  cancel() {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    let t = this.__destroy_into_raw();
    c(), i.intounderlyingsource_cancel(t);
  }
  pull(t) {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    let n;
    return c(), n = i.intounderlyingsource_pull(this.__wbg_ptr, s(t)), p(n);
  }
};
Symbol.dispose && (z.prototype[Symbol.dispose] = z.prototype.free);
var O = class {
  static {
    __name(this, "O");
  }
  __destroy_into_raw() {
    let t = this.__wbg_ptr;
    return this.__wbg_ptr = 0, De.unregister(this), t;
  }
  free() {
    let t = this.__destroy_into_raw();
    c(), i.__wbg_minifyconfig_free(t, 0);
  }
  get css() {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    let t;
    return c(), t = i.__wbg_get_minifyconfig_css(this.__wbg_ptr), t !== 0;
  }
  get html() {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    let t;
    return c(), t = i.__wbg_get_minifyconfig_html(this.__wbg_ptr), t !== 0;
  }
  get js() {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    let t;
    return c(), t = i.__wbg_get_minifyconfig_js(this.__wbg_ptr), t !== 0;
  }
  set css(t) {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    c(), i.__wbg_set_minifyconfig_css(this.__wbg_ptr, t);
  }
  set html(t) {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    c(), i.__wbg_set_minifyconfig_html(this.__wbg_ptr, t);
  }
  set js(t) {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    c(), i.__wbg_set_minifyconfig_js(this.__wbg_ptr, t);
  }
};
Symbol.dispose && (O.prototype[Symbol.dispose] = O.prototype.free);
var M = class {
  static {
    __name(this, "M");
  }
  __destroy_into_raw() {
    let t = this.__wbg_ptr;
    return this.__wbg_ptr = 0, Ce.unregister(this), t;
  }
  free() {
    let t = this.__destroy_into_raw();
    c(), i.__wbg_r2range_free(t, 0);
  }
  get length() {
    try {
      if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
      let r = i.__wbindgen_add_to_stack_pointer(-16);
      c(), i.__wbg_get_r2range_length(r, this.__wbg_ptr);
      var t = f().getInt32(r + 0, true), n = f().getFloat64(r + 8, true);
      return t === 0 ? void 0 : n;
    } finally {
      i.__wbindgen_add_to_stack_pointer(16);
    }
  }
  get offset() {
    try {
      if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
      let r = i.__wbindgen_add_to_stack_pointer(-16);
      c(), i.__wbg_get_r2range_offset(r, this.__wbg_ptr);
      var t = f().getInt32(r + 0, true), n = f().getFloat64(r + 8, true);
      return t === 0 ? void 0 : n;
    } finally {
      i.__wbindgen_add_to_stack_pointer(16);
    }
  }
  get suffix() {
    try {
      if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
      let r = i.__wbindgen_add_to_stack_pointer(-16);
      c(), i.__wbg_get_r2range_suffix(r, this.__wbg_ptr);
      var t = f().getInt32(r + 0, true), n = f().getFloat64(r + 8, true);
      return t === 0 ? void 0 : n;
    } finally {
      i.__wbindgen_add_to_stack_pointer(16);
    }
  }
  set length(t) {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    c(), i.__wbg_set_r2range_length(this.__wbg_ptr, !g(t), g(t) ? 0 : t);
  }
  set offset(t) {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    c(), i.__wbg_set_r2range_offset(this.__wbg_ptr, !g(t), g(t) ? 0 : t);
  }
  set suffix(t) {
    if (this.__wbg_inst !== void 0 && this.__wbg_inst !== u) throw new Error("Invalid stale object from previous Wasm instance");
    c(), i.__wbg_set_r2range_suffix(this.__wbg_ptr, !g(t), g(t) ? 0 : t);
  }
};
Symbol.dispose && (M.prototype[Symbol.dispose] = M.prototype.free);
function G() {
  u++, x = null, F = null, typeof W < "u" && (W = 0), typeof l < "u" && (l = 0), typeof w < "u" && (w = new Array(1024).fill(void 0), w = w.concat([void 0, null, true, false]), typeof I < "u" && (I = w.length)), B = false, wasmInstance = new WebAssembly.Instance(wasmModule, __wbg_get_imports()), i = wasmInstance.exports, i.__wbindgen_start();
}
__name(G, "G");
function Y() {
  let e;
  return c(), e = i.__worker_init_state(), p(e);
}
__name(Y, "Y");
function C(e, t, n) {
  let r;
  return c(), r = i.fetch(s(e), s(t), s(n)), p(r);
}
__name(C, "C");
function K() {
  c(), i.init();
}
__name(K, "K");
function Q(e, t) {
  let n = String(_(t)), r = v(n, i.__wbindgen_export, i.__wbindgen_export2), o = l;
  f().setInt32(e + 4, o, true), f().setInt32(e + 0, r, true);
}
__name(Q, "Q");
function Z(e, t) {
  let n = D(_(t)), r = v(n, i.__wbindgen_export, i.__wbindgen_export2), o = l;
  f().setInt32(e + 4, o, true), f().setInt32(e + 0, r, true);
}
__name(Z, "Z");
function tt(e, t) {
  return _(e) in _(t);
}
__name(tt, "tt");
function et(e) {
  return typeof _(e) == "function";
}
__name(et, "et");
function nt(e) {
  return _(e) === null;
}
__name(nt, "nt");
function rt(e) {
  return _(e) == null;
}
__name(rt, "rt");
function _t(e) {
  return _(e) === void 0;
}
__name(_t, "_t");
function it() {
  B = true;
}
__name(it, "it");
function ot(e, t) {
  let n = _(t), r = typeof n == "string" ? n : void 0;
  var o = g(r) ? 0 : v(r, i.__wbindgen_export, i.__wbindgen_export2), b = l;
  f().setInt32(e + 4, b, true), f().setInt32(e + 0, o, true);
}
__name(ot, "ot");
function st(e, t) {
  throw new Error(y(e, t));
}
__name(st, "st");
function ct(e) {
  _(e)._wbg_cb_unref();
}
__name(ct, "ct");
function ut() {
  return d(function(e) {
    return _(e).bodyUsed;
  }, arguments);
}
__name(ut, "ut");
function ft() {
  return d(function(e) {
    let t = _(e).body;
    return s(t);
  }, arguments);
}
__name(ft, "ft");
function at(e) {
  let t = _(e).body;
  return g(t) ? 0 : s(t);
}
__name(at, "at");
function bt(e) {
  let t = _(e).buffer;
  return s(t);
}
__name(bt, "bt");
function dt(e) {
  let t = _(e).byobRequest;
  return g(t) ? 0 : s(t);
}
__name(dt, "dt");
function gt(e) {
  return _(e).byteLength;
}
__name(gt, "gt");
function wt(e) {
  return _(e).byteOffset;
}
__name(wt, "wt");
function pt() {
  return d(function(e, t, n) {
    let r = _(e).call(_(t), _(n));
    return s(r);
  }, arguments);
}
__name(pt, "pt");
function lt(e) {
  let t = _(e).cancel();
  return s(t);
}
__name(lt, "lt");
function ht(e, t) {
  let n = _(e).catch(_(t));
  return s(n);
}
__name(ht, "ht");
function yt(e) {
  let t = _(e).cause;
  return s(t);
}
__name(yt, "yt");
function mt() {
  return d(function(e) {
    let t = _(e).cf;
    return g(t) ? 0 : s(t);
  }, arguments);
}
__name(mt, "mt");
function xt() {
  return d(function(e) {
    _(e).close();
  }, arguments);
}
__name(xt, "xt");
function It() {
  return d(function(e) {
    _(e).close();
  }, arguments);
}
__name(It, "It");
function vt(e) {
  let t = _(e).constructor;
  return s(t);
}
__name(vt, "vt");
function Et() {
  return d(function(e, t) {
    _(e).enqueue(_(t));
  }, arguments);
}
__name(Et, "Et");
function Ft(e) {
  console.error(_(e));
}
__name(Ft, "Ft");
function jt(e, t) {
  console.error(_(e), _(t));
}
__name(jt, "jt");
function Wt() {
  return d(function(e) {
    let t = _(e).getReader();
    return s(t);
  }, arguments);
}
__name(Wt, "Wt");
function St() {
  return d(function(e, t) {
    let n = Reflect.get(_(e), _(t));
    return g(n) ? 0 : s(n);
  }, arguments);
}
__name(St, "St");
function Rt() {
  return d(function(e, t, n, r) {
    let o, b;
    try {
      o = t, b = n;
      let a = _(e).get(y(t, n), p(r));
      return s(a);
    } finally {
      c(), i.__wbindgen_export4(o, b, 1);
    }
  }, arguments);
}
__name(Rt, "Rt");
function kt() {
  return d(function(e, t) {
    let n = Reflect.get(_(e), _(t));
    return s(n);
  }, arguments);
}
__name(kt, "kt");
function At() {
  return d(function(e, t, n, r) {
    let o = _(t).get(y(n, r));
    var b = g(o) ? 0 : v(o, i.__wbindgen_export, i.__wbindgen_export2), a = l;
    f().setInt32(e + 4, a, true), f().setInt32(e + 0, b, true);
  }, arguments);
}
__name(At, "At");
function zt(e) {
  let t = _(e).done;
  return g(t) ? 16777215 : t ? 1 : 0;
}
__name(zt, "zt");
function Ot(e) {
  let t = _(e).value;
  return s(t);
}
__name(Ot, "Ot");
function Mt() {
  return d(function(e, t, n) {
    let r, o;
    try {
      r = t, o = n;
      let b = _(e).head(y(t, n));
      return s(b);
    } finally {
      c(), i.__wbindgen_export4(r, o, 1);
    }
  }, arguments);
}
__name(Mt, "Mt");
function Tt(e) {
  let t = _(e).headers;
  return s(t);
}
__name(Tt, "Tt");
function Ut() {
  return d(function(e, t) {
    let n = _(t).httpEtag, r = v(n, i.__wbindgen_export, i.__wbindgen_export2), o = l;
    f().setInt32(e + 4, o, true), f().setInt32(e + 0, r, true);
  }, arguments);
}
__name(Ut, "Ut");
function Lt(e) {
  return _(e).instanceId;
}
__name(Lt, "Lt");
function qt(e) {
  let t;
  try {
    t = _(e) instanceof Error;
  } catch {
    t = false;
  }
  return t;
}
__name(qt, "qt");
function Dt(e) {
  let t;
  try {
    t = _(e) instanceof ReadableStream;
  } catch {
    t = false;
  }
  return t;
}
__name(Dt, "Dt");
function Ct(e) {
  return _(e).length;
}
__name(Ct, "Ct");
function Bt(e) {
  let t = _(e).message;
  return s(t);
}
__name(Bt, "Bt");
function $t(e, t) {
  let n = _(t).method, r = v(n, i.__wbindgen_export, i.__wbindgen_export2), o = l;
  f().setInt32(e + 4, o, true), f().setInt32(e + 0, r, true);
}
__name($t, "$t");
function Nt(e) {
  let t = _(e).name;
  return s(t);
}
__name(Nt, "Nt");
function Vt(e) {
  let t = _(e).name;
  return s(t);
}
__name(Vt, "Vt");
function Ht() {
  return d(function() {
    let e = new Headers();
    return s(e);
  }, arguments);
}
__name(Ht, "Ht");
function Pt(e, t) {
  let n = new Error(y(e, t));
  return s(n);
}
__name(Pt, "Pt");
function Jt() {
  let e = new Object();
  return s(e);
}
__name(Jt, "Jt");
function Xt(e, t) {
  try {
    var n = { a: e, b: t }, r = /* @__PURE__ */ __name((b, a) => {
      let h = n.a;
      n.a = 0;
      try {
        return ze(h, n.b, b, a);
      } finally {
        n.a = h;
      }
    }, "r");
    let o = new Promise(r);
    return s(o);
  } finally {
    n.a = 0;
  }
}
__name(Xt, "Xt");
function Gt(e, t, n) {
  let r = new Uint8Array(_(e), t >>> 0, n >>> 0);
  return s(r);
}
__name(Gt, "Gt");
function Yt(e) {
  let t = new Uint8Array(e >>> 0);
  return s(t);
}
__name(Yt, "Yt");
function Kt() {
  return d(function(e, t) {
    let n = new Response(_(e), _(t));
    return s(n);
  }, arguments);
}
__name(Kt, "Kt");
function Qt() {
  return d(function(e, t) {
    let n = new Response(_(e), _(t));
    return s(n);
  }, arguments);
}
__name(Qt, "Qt");
function Zt() {
  return d(function(e, t, n) {
    let r = new Response(e === 0 ? void 0 : y(e, t), _(n));
    return s(r);
  }, arguments);
}
__name(Zt, "Zt");
function te(e, t, n) {
  Uint8Array.prototype.set.call(H(e, t), _(n));
}
__name(te, "te");
function ee(e) {
  let t = _(e).queueMicrotask;
  return s(t);
}
__name(ee, "ee");
function ne(e) {
  queueMicrotask(_(e));
}
__name(ne, "ne");
function re(e) {
  let t = _(e).read();
  return s(t);
}
__name(re, "re");
function _e(e) {
  _(e).releaseLock();
}
__name(_e, "_e");
function ie(e) {
  let t = Promise.resolve(_(e));
  return s(t);
}
__name(ie, "ie");
function oe() {
  return d(function(e, t) {
    _(e).respond(t >>> 0);
  }, arguments);
}
__name(oe, "oe");
function se(e, t, n) {
  _(e).set(H(t, n));
}
__name(se, "se");
function ce() {
  return d(function(e, t, n, r, o) {
    _(e).set(y(t, n), y(r, o));
  }, arguments);
}
__name(ce, "ce");
function ue() {
  return d(function(e, t, n) {
    return Reflect.set(_(e), _(t), _(n));
  }, arguments);
}
__name(ue, "ue");
function fe(e, t) {
  _(e).criticalError = t !== 0;
}
__name(fe, "fe");
function ae(e, t) {
  _(e).headers = _(t);
}
__name(ae, "ae");
function be(e, t) {
  _(e).instanceId = t >>> 0;
}
__name(be, "be");
function de(e, t) {
  _(e).status = t;
}
__name(de, "de");
function ge() {
  return d(function(e) {
    return _(e).size;
  }, arguments);
}
__name(ge, "ge");
function we() {
  let e = typeof global > "u" ? null : global;
  return g(e) ? 0 : s(e);
}
__name(we, "we");
function pe() {
  let e = typeof globalThis > "u" ? null : globalThis;
  return g(e) ? 0 : s(e);
}
__name(pe, "pe");
function le() {
  return s(L);
}
__name(le, "le");
function he() {
  let e = typeof self > "u" ? null : self;
  return g(e) ? 0 : s(e);
}
__name(he, "he");
function ye() {
  let e = typeof window > "u" ? null : window;
  return g(e) ? 0 : s(e);
}
__name(ye, "ye");
function me(e, t, n) {
  let r = _(e).then(_(t), _(n));
  return s(r);
}
__name(me, "me");
function xe(e, t) {
  let n = _(e).then(_(t));
  return s(n);
}
__name(xe, "xe");
function Ie(e, t) {
  let n = _(t).url, r = v(n, i.__wbindgen_export, i.__wbindgen_export2), o = l;
  f().setInt32(e + 4, o, true), f().setInt32(e + 0, r, true);
}
__name(Ie, "Ie");
function ve(e) {
  let t = _(e).view;
  return g(t) ? 0 : s(t);
}
__name(ve, "ve");
function Ee() {
  return d(function(e, t) {
    let n = _(e).writeHttpMetadata(p(t));
    return s(n);
  }, arguments);
}
__name(Ee, "Ee");
function Fe(e, t) {
  let n = P(e, t, Ae);
  return s(n);
}
__name(Fe, "Fe");
function je(e, t) {
  let n = P(e, t, Oe);
  return s(n);
}
__name(je, "je");
function We(e) {
  return s(e);
}
__name(We, "We");
function Se(e, t) {
  let n = y(e, t);
  return s(n);
}
__name(Se, "Se");
function Re(e) {
  let t = _(e);
  return s(t);
}
__name(Re, "Re");
function ke(e) {
  p(e);
}
__name(ke, "ke");
function c() {
  if (B) {
    G();
    return;
  }
}
__name(c, "c");
function Ae(e, t, n) {
  c(), i.__wasm_bindgen_func_elem_653(e, t, s(n));
}
__name(Ae, "Ae");
function ze(e, t, n, r) {
  c(), i.__wasm_bindgen_func_elem_1340(e, t, s(n), s(r));
}
__name(ze, "ze");
function Oe(e, t, n) {
  try {
    let b = i.__wbindgen_add_to_stack_pointer(-16);
    c(), i.__wasm_bindgen_func_elem_1332(b, e, t, s(n));
    var r = f().getInt32(b + 0, true), o = f().getInt32(b + 4, true);
    if (o) throw p(r);
  } finally {
    i.__wbindgen_add_to_stack_pointer(16);
  }
}
__name(Oe, "Oe");
var Me = ["bytes"];
var u = 0;
var Te = typeof FinalizationRegistry > "u" ? { register: /* @__PURE__ */ __name(() => {
}, "register"), unregister: /* @__PURE__ */ __name(() => {
}, "unregister") } : new FinalizationRegistry(({ ptr: e, instance: t }) => {
  t === u && i.__wbg_containerstartupoptions_free(e, 1);
});
var Ue = typeof FinalizationRegistry > "u" ? { register: /* @__PURE__ */ __name(() => {
}, "register"), unregister: /* @__PURE__ */ __name(() => {
}, "unregister") } : new FinalizationRegistry(({ ptr: e, instance: t }) => {
  t === u && i.__wbg_intounderlyingbytesource_free(e, 1);
});
var Le = typeof FinalizationRegistry > "u" ? { register: /* @__PURE__ */ __name(() => {
}, "register"), unregister: /* @__PURE__ */ __name(() => {
}, "unregister") } : new FinalizationRegistry(({ ptr: e, instance: t }) => {
  t === u && i.__wbg_intounderlyingsink_free(e, 1);
});
var qe = typeof FinalizationRegistry > "u" ? { register: /* @__PURE__ */ __name(() => {
}, "register"), unregister: /* @__PURE__ */ __name(() => {
}, "unregister") } : new FinalizationRegistry(({ ptr: e, instance: t }) => {
  t === u && i.__wbg_intounderlyingsource_free(e, 1);
});
var De = typeof FinalizationRegistry > "u" ? { register: /* @__PURE__ */ __name(() => {
}, "register"), unregister: /* @__PURE__ */ __name(() => {
}, "unregister") } : new FinalizationRegistry(({ ptr: e, instance: t }) => {
  t === u && i.__wbg_minifyconfig_free(e, 1);
});
var Ce = typeof FinalizationRegistry > "u" ? { register: /* @__PURE__ */ __name(() => {
}, "register"), unregister: /* @__PURE__ */ __name(() => {
}, "unregister") } : new FinalizationRegistry(({ ptr: e, instance: t }) => {
  t === u && i.__wbg_r2range_free(e, 1);
});
function s(e) {
  I === w.length && w.push(w.length + 1);
  let t = I;
  return I = w[t], w[t] = e, t;
}
__name(s, "s");
var V = typeof FinalizationRegistry > "u" ? { register: /* @__PURE__ */ __name(() => {
}, "register"), unregister: /* @__PURE__ */ __name(() => {
}, "unregister") } : new FinalizationRegistry((e) => {
  e.instance === u && i.__wbindgen_export5(e.a, e.b);
});
function D(e) {
  let t = typeof e;
  if (t == "number" || t == "boolean" || e == null) return `${e}`;
  if (t == "string") return `"${e}"`;
  if (t == "symbol") {
    let o = e.description;
    return o == null ? "Symbol" : `Symbol(${o})`;
  }
  if (t == "function") {
    let o = e.name;
    return typeof o == "string" && o.length > 0 ? `Function(${o})` : "Function";
  }
  if (Array.isArray(e)) {
    let o = e.length, b = "[";
    o > 0 && (b += D(e[0]));
    for (let a = 1; a < o; a++) b += ", " + D(e[a]);
    return b += "]", b;
  }
  let n = /\[object ([^\]]+)\]/.exec(toString.call(e)), r;
  if (n && n.length > 1) r = n[1];
  else return toString.call(e);
  if (r == "Object") try {
    return "Object(" + JSON.stringify(e) + ")";
  } catch {
    return "Object";
  }
  return e instanceof Error ? `${e.name}: ${e.message}
${e.stack}` : r;
}
__name(D, "D");
function Be(e) {
  e < 1028 || (w[e] = I, I = e);
}
__name(Be, "Be");
function $e(e, t) {
  e = e >>> 0;
  let n = f(), r = [];
  for (let o = e; o < e + 4 * t; o += 4) r.push(p(n.getUint32(o, true)));
  return r;
}
__name($e, "$e");
function H(e, t) {
  return e = e >>> 0, j().subarray(e / 1, e / 1 + t);
}
__name(H, "H");
var x = null;
function f() {
  return (x === null || x.buffer.detached === true || x.buffer.detached === void 0 && x.buffer !== i.memory.buffer) && (x = new DataView(i.memory.buffer)), x;
}
__name(f, "f");
function y(e, t) {
  return He(e >>> 0, t);
}
__name(y, "y");
var F = null;
function j() {
  return (F === null || F.byteLength === 0) && (F = new Uint8Array(i.memory.buffer)), F;
}
__name(j, "j");
function _(e) {
  return w[e];
}
__name(_, "_");
function d(e, t) {
  try {
    return e.apply(this, t);
  } catch (n) {
    i.__wbindgen_export3(s(n));
  }
}
__name(d, "d");
var w = new Array(1024).fill(void 0);
w.push(void 0, null, true, false);
var I = w.length;
function g(e) {
  return e == null;
}
__name(g, "g");
function P(e, t, n) {
  let r = { a: e, b: t, cnt: 1, instance: u }, o = /* @__PURE__ */ __name((...b) => {
    if (r.instance !== u) throw new Error("Cannot invoke closure from previous WASM instance");
    r.cnt++;
    let a = r.a;
    r.a = 0;
    try {
      return n(a, r.b, ...b);
    } finally {
      r.a = a, o._wbg_cb_unref();
    }
  }, "o");
  return o._wbg_cb_unref = () => {
    --r.cnt === 0 && (i.__wbindgen_export5(r.a, r.b), r.a = 0, V.unregister(r));
  }, V.register(o, r, r), o;
}
__name(P, "P");
function Ne(e, t) {
  let n = t(e.length * 4, 4) >>> 0, r = f();
  for (let o = 0; o < e.length; o++) r.setUint32(n + 4 * o, s(e[o]), true);
  return l = e.length, n;
}
__name(Ne, "Ne");
function v(e, t, n) {
  if (n === void 0) {
    let h = S.encode(e), E = t(h.length, 1) >>> 0;
    return j().subarray(E, E + h.length).set(h), l = h.length, E;
  }
  let r = e.length, o = t(r, 1) >>> 0, b = j(), a = 0;
  for (; a < r; a++) {
    let h = e.charCodeAt(a);
    if (h > 127) break;
    b[o + a] = h;
  }
  if (a !== r) {
    a !== 0 && (e = e.slice(a)), o = n(o, r, r = a + e.length * 3, 1) >>> 0;
    let h = j().subarray(o + a, o + r), E = S.encodeInto(e, h);
    a += E.written, o = n(o, r, a, 1) >>> 0;
  }
  return l = a, o;
}
__name(v, "v");
var B = false;
function p(e) {
  let t = _(e);
  return Be(e), t;
}
__name(p, "p");
var T = new TextDecoder("utf-8", { ignoreBOM: true, fatal: true });
T.decode();
var Ve = 2146435072;
var W = 0;
function He(e, t) {
  return W += t, W >= Ve && (T = new TextDecoder("utf-8", { ignoreBOM: true, fatal: true }), T.decode(), W = t), T.decode(j().subarray(e, e + t));
}
__name(He, "He");
var S = new TextEncoder();
"encodeInto" in S || (S.encodeInto = function(e, t) {
  let n = S.encode(e);
  return t.set(n), { read: e.length, written: n.length };
});
var l = 0;
var i;
function $(e) {
  i = e;
}
__name($, "$");
var J = new WebAssembly.Instance(Pe, { "./index_bg.js": m, "./snippets/worker-735232a012c24f5a/inline0.js": q });
$(J.exports);
J.exports.__wbindgen_start?.();
var U = class extends Je {
  static {
    __name(this, "U");
  }
  async fetch(t) {
    return await C(t, this.env, this.ctx);
  }
  async queue(t) {
    return await (void 0)(t, this.env, this.ctx);
  }
  async scheduled(t) {
    return await (void 0)(t, this.env, this.ctx);
  }
};
var Xe = ["IntoUnderlyingByteSource", "IntoUnderlyingSink", "IntoUnderlyingSource", "MinifyConfig", "PolishConfig", "R2Range", "RequestRedirect", "fetch", "queue", "scheduled", "getMemory"];
Object.keys(m).map((e) => {
  Xe.includes(e) | e.startsWith("__") || (U.prototype[e] = m[e]);
});
var Ze = U;
export {
  R as ContainerStartupOptions,
  k as IntoUnderlyingByteSource,
  A as IntoUnderlyingSink,
  z as IntoUnderlyingSource,
  O as MinifyConfig,
  M as R2Range,
  Q as __wbg_String_8564e559799eccda,
  Z as __wbg___wbindgen_debug_string_0e68cf47c9cbd9b0,
  tt as __wbg___wbindgen_in_50072d4d6e45c193,
  et as __wbg___wbindgen_is_function_fcda5e3902d732fe,
  nt as __wbg___wbindgen_is_null_5160b3e381865372,
  rt as __wbg___wbindgen_is_null_or_undefined_4b0bf0653367120c,
  _t as __wbg___wbindgen_is_undefined_8c687d0b90d5b524,
  it as __wbg___wbindgen_reinit_0256f5d04898665d,
  ot as __wbg___wbindgen_string_get_92ab86bb19cbc12f,
  st as __wbg___wbindgen_throw_5d9e815e6fdf150f,
  ct as __wbg__wbg_cb_unref_997e73d32238e655,
  ut as __wbg_bodyUsed_930a158a8e1aec0f,
  ft as __wbg_body_60100aca0566fdf4,
  at as __wbg_body_98d7808e69835fb7,
  bt as __wbg_buffer_4a989bded7035f57,
  dt as __wbg_byobRequest_f161fc37241dd3d4,
  gt as __wbg_byteLength_0ddb1795e2c7e689,
  wt as __wbg_byteOffset_46eb015f52b6ad7d,
  pt as __wbg_call_6bcf8d3e20937e46,
  lt as __wbg_cancel_c076350a7e8954be,
  ht as __wbg_catch_e2134ef6dcdb69ed,
  yt as __wbg_cause_36bd622854f9b3a4,
  mt as __wbg_cf_c7b0f56b1787c078,
  xt as __wbg_close_22882088c136df25,
  It as __wbg_close_8b609460dd26e367,
  vt as __wbg_constructor_207e5664c25e1af9,
  Et as __wbg_enqueue_6cb545d22db14f33,
  Ft as __wbg_error_756c5934221e6fee,
  jt as __wbg_error_b7e73e671a58a488,
  Wt as __wbg_getReader_bb0230851fbf986b,
  St as __wbg_get_423ea1f7ff5c8d64,
  Rt as __wbg_get_94d18d21679927f6,
  kt as __wbg_get_989d0a1309644f2b,
  At as __wbg_get_b5793eadbdf6b0ae,
  zt as __wbg_get_done_a668aa62d81fad70,
  Ot as __wbg_get_value_6c62a77c168d825a,
  Mt as __wbg_head_70d4ddc719656838,
  Tt as __wbg_headers_a1e9854406915ee7,
  Ut as __wbg_httpEtag_3ae37bea99ffb11e,
  Lt as __wbg_instanceId_232fcea31f0d9b32,
  qt as __wbg_instanceof_Error_fe6fa771c78ee4cf,
  Dt as __wbg_instanceof_ReadableStream_07dda5955244a495,
  Ct as __wbg_length_31bdaf014f5fbde2,
  Bt as __wbg_message_1cbc5bc03dcf1dee,
  $t as __wbg_method_f625716bf1f7b540,
  Nt as __wbg_name_b4e1ee96e711fbdf,
  Vt as __wbg_name_b9d8f2ea16b22045,
  Ht as __wbg_new_0afe64b4dc16ab74,
  Pt as __wbg_new_a32a1ab6c6655abe,
  Jt as __wbg_new_bebc3f4757acf305,
  Xt as __wbg_new_typed_6f8b0d724fe26c07,
  Gt as __wbg_new_with_byte_offset_and_length_492c969e8b5da8a4,
  Yt as __wbg_new_with_length_5ffeddb9d9fbb96f,
  Kt as __wbg_new_with_opt_buffer_source_and_init_935ee9753c594708,
  Qt as __wbg_new_with_opt_readable_stream_and_init_652b1d60607d75aa,
  Zt as __wbg_new_with_opt_str_and_init_cbaf7a52e7dc0b00,
  te as __wbg_prototypesetcall_ae9f5e7459250748,
  ee as __wbg_queueMicrotask_85c90f6987555d65,
  ne as __wbg_queueMicrotask_f6a1fa10b81d1fc0,
  re as __wbg_read_31091533ffadf971,
  _e as __wbg_releaseLock_1538945f5f183d9f,
  ie as __wbg_resolve_35ec7e0c6af4c82c,
  oe as __wbg_respond_83a71686e927ca32,
  se as __wbg_set_5f2ad37e5e02dc7b,
  ce as __wbg_set_7923e5ea63b41e6b,
  ue as __wbg_set_a377297433dfea63,
  fe as __wbg_set_criticalError_e391f6c6a38c6d68,
  ae as __wbg_set_headers_dfe6a763facd3dcc,
  be as __wbg_set_instanceId_b0493682e07f9aa4,
  de as __wbg_set_status_74351f5228412c33,
  $ as __wbg_set_wasm,
  ge as __wbg_size_9136fb55c85bae0e,
  we as __wbg_static_accessor_GLOBAL_8eb4cd83130a11a0,
  pe as __wbg_static_accessor_GLOBAL_THIS_1e7044f654e934db,
  le as __wbg_static_accessor_INIT_STATE_86fe3c7381036fc1,
  he as __wbg_static_accessor_SELF_d8b50611246a6d92,
  ye as __wbg_static_accessor_WINDOW_fd0bc376bf0f8b42,
  me as __wbg_then_7a850dae4493f353,
  xe as __wbg_then_b830475380919203,
  Ie as __wbg_url_8d180a4a6996e22d,
  ve as __wbg_view_d8c7b26e4d4650f1,
  Ee as __wbg_writeHttpMetadata_876c85f049293fec,
  Fe as __wbindgen_generic_0000000000000001,
  je as __wbindgen_generic_0000000000000002,
  We as __wbindgen_generic_0000000000000003,
  Se as __wbindgen_generic_0000000000000004,
  Re as __wbindgen_object_clone_ref,
  ke as __wbindgen_object_drop_ref,
  Y as __worker_init_state,
  Ze as default,
  C as fetch,
  K as init,
  Pe as wasmModule
};
//# sourceMappingURL=shim.js.map
