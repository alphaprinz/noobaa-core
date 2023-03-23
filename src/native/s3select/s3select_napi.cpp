/* Copyright (C) 2016 NooBaa */

#include "../../../src/native/third_party/isa-l/include/crc.h"
#include "../../../submodules/s3select/include/s3select.h"
#include "../util/common.h"
#include "../util/napi.h"
#include <arpa/inet.h>
//#include "s3select_parquet.cpp" //TODO - h?

namespace noobaa
{

const char *CSV_FORMAT = "CSV";
const char *JSON_FORMAT = "JSON";
const char *PARQUET_FORMAT = "Parquet";

class S3SelectNapi;

class SelectWorker : public Napi::AsyncWorker
{
public:
    Napi::ObjectReference _args_ref;
    Napi::Promise::Deferred _deferred;
    Napi::ObjectWrap<S3SelectNapi>& _wrap;
    s3selectEngine::csv_object *_csv_object;
    s3selectEngine::json_object *_json_object;
    std::string _select;
    const char* _buffer;
    const unsigned int _buffer_len;
    const bool _is_flush;
    const std::string _input_format;
    const uint8_t* _headers_bytes;
    const uint32_t _headers_len;

    SelectWorker(const Napi::CallbackInfo& info,
        Napi::ObjectWrap<S3SelectNapi>& wrap,
        s3selectEngine::csv_object* csv_object,
        s3selectEngine::json_object* json_object,
        const char* buffer,
        const unsigned int buffer_len,
        const bool is_flush,
        const std::string input_format,
        const uint8_t* headers_bytes,
        const uint32_t headers_len)
        : AsyncWorker(info.Env())
        , _args_ref(Napi::Persistent(Napi::Object::New(info.Env())))
        , _wrap(wrap)
        , _csv_object(csv_object)
        , _json_object(json_object)
        , _buffer(buffer)
        , _buffer_len(buffer_len)
        , _is_flush(is_flush)
        , _input_format(input_format)
        , _deferred(info.Env())
        , _headers_bytes(headers_bytes)
        , _headers_len(headers_len)
    {
        //take a ref on args to make sure they are not GCed until worker is done
        uint32_t i;
        for (i = 0; i < info.Length(); ++i) {
            _args_ref.Set(i, info[i]);
        }
        //prevent GC of S3SelectNapi object by JS until worker is done
        _wrap.Ref();
    }

    ~SelectWorker()
    {
        //Worker is done, unref the ref we took in ctor
        _wrap.Unref();
    }

    void Execute() override
    {
        int rc;
        // TODO - if we get total size on beginning, i think we can remove the is_flush if
        if (CSV_FORMAT == _input_format) {
            if (_is_flush) {
                rc = _csv_object->run_s3select_on_stream(_select, nullptr, 0, 0);
            } else {
                rc = _csv_object->run_s3select_on_stream(_select, _buffer, _buffer_len, SIZE_MAX);
            }
        } else {
            if (_is_flush) {
                rc = _json_object->run_s3select_on_stream(_select, nullptr, 0, 0);
            } else {
                rc = _json_object->run_s3select_on_stream(_select, _buffer, _buffer_len, SIZE_MAX);
            }
        }
        if (rc < 0) {
            if (CSV_FORMAT == _input_format) {
                SetError(_csv_object->get_error_description());
            } else {
                // SetError(json_object.get_error_description()); //TODO - they added a getter, use after submodule is updated
                SetError("failed to select from json");
            }
        }

        /*std::cout << "select res = " << this->select << std::endl;
        std::cout.flush();*/
    }

    void OnOK() override
    {
        Napi::Env env = Env();
        // in case of empty select result (ie, no rows in current buffer matched sql condition), return null
        if (_select.empty()) {
            _deferred.Resolve(env.Null());
            return;
        }

        Napi::Object result = Napi::Object::New(env);
        Napi::Value select_buf = Napi::Buffer<char>::/*New*/ Copy(env, _select.data(), _select.length());
        result.Set("select", select_buf);

        uint32_t prelude[3];
        int32_t prelude_crc, message_crc, prelude_crc_BE;
        prelude[0] = htonl(_select.length() + _headers_len + 16);
        prelude[1] = htonl(_headers_len);
        prelude_crc = crc32_gzip_refl_base(0, (uint8_t*)prelude, 8);
        result.Set("prelude_crc", prelude_crc);
        prelude_crc_BE = htonl(prelude_crc);
        /*std::cout << "sz = " << sizeof(headers_bytes) << ", native prelude_crc = " << prelude_crc << ", prelude = " << std::hex << std::setfill('0') << std::setw(2) << prelude[0] << prelude[1];
        std::cout << std::endl;
        std::cout.flush();*/
        message_crc = crc32_gzip_refl_base(prelude_crc, (uint8_t*)&prelude_crc_BE, 4);
        message_crc = crc32_gzip_refl_base(message_crc, const_cast<uint8_t*>(_headers_bytes), _headers_len);
        message_crc = crc32_gzip_refl_base(message_crc, (uint8_t*)_select.data(), _select.length());
        result.Set("message_crc", message_crc);

        _deferred.Resolve(result);
    }

    void OnError(Napi::Error const& error) override
    {
        Napi::Env env = Env();
        auto obj = error.Value();
        _deferred.Reject(obj);
    }
};

class RangeWorker : public Napi::AsyncWorker{
public:

    void *_buffer = nullptr;
    int64_t _length = 0;
    int64_t _start = 0;
    bool _is_done = false;
    //const Napi::Object &_range_request_context;
    const napi_threadsafe_function &_range_request_napi;
    const napi_threadsafe_function &_is_done_napi;
    uint64_t _res = 0;
    Napi::Promise *_promise_from_js = nullptr;
    //Napi::Env env;
    napi_ref _rrc_nf;

    RangeWorker(
        const Napi::CallbackInfo& info
        /*const Napi::Env &env,
        const int64_t start,
        const int64_t length,
        const void* buffer,*/
        //,const Napi::Object range_request_context)
        ,napi_threadsafe_function &range_request_napi
        ,napi_threadsafe_function &is_done_napi
        ,napi_ref rrc_nf)
        : AsyncWorker(info.Env())
        //, _start(start)
        //, _length(length)
        //, _buffer(buffer)
        //, _range_request_context(range_request_context),
        ,_range_request_napi(range_request_napi)
        ,_is_done_napi(is_done_napi)
        ,_rrc_nf(rrc_nf)
        //,env(info.Env())
        {
        std::cout << "range WORKER ctor = " << _start << " ,length = " << _length << std::endl;
        std::cout.flush();
        }
    
    void Execute() override {
        Napi::Env env = Env();

        std::cout << "range WORKER req start = " << _start << " ,length = " << _length << std::endl;
        std::cout.flush();

        napi_acquire_threadsafe_function(_range_request_napi);
        std::cout << "range WORKER req acquire = " << _start << " ,length = " << _length << std::endl;
        std::cout.flush();
        napi_call_threadsafe_function(
            _range_request_napi,
            this,
            napi_tsfn_blocking);

        std::cout << "range WORKER req call tsfn = " << _start << " ,length = " << _length << std::endl;
        std::cout.flush();

        napi_value range_request_context;
        napi_status status;
        uint32_t dontcare;
        status = napi_reference_ref(env, _rrc_nf, &dontcare);
        std::cout << "range WORKER ns ref ref = " << status << std::endl;
        std::cout.flush();
        status = napi_get_reference_value(env, _rrc_nf, &range_request_context);
    
        std::cout << "range WORKER ns get ref = " << status << std::endl;
        std::cout.flush();

        Napi::Value rrc_nv(env, range_request_context);
        Napi::Object rrc = rrc_nv.As<Napi::Object>();
        while(!_is_done){
            std::cout << "_is_done = " << _is_done << std::endl;
            std::cout.flush();
            sleep(20);
            _is_done = rrc.Get("is_done").As<Napi::Boolean>();
        }

    }

    void OnOK() override {
        _is_done = true;
    }

    void OnError(Napi::Error const& error) override {
        _is_done = true;
    }
};

Napi::Value Range_Request_Thread_Safe_CB(const Napi::CallbackInfo &info) {
    std::cout << "empty callback with " << info[0].ToString().Utf8Value().c_str() <<std::endl;
    std::cout.flush();

    Napi::Value info0 = info[0], infot = info.This();
    std::cout << "info0 IsPromise = " << info0.IsPromise() << ", this.isp " << infot.IsPromise() << std::endl;
    std::cout.flush();
    std::cout << "info0 has is_done = " << info0.As<Napi::Object>().Has("is_done") << std::endl;
    std::cout.flush();
    std::cout << "empty callback with this " << infot.ToString().Utf8Value().c_str() <<std::endl;
    std::cout.flush();
    Napi::Object rrc = info0.As<Napi::Object>();
    std::cout << "info0.is_done = " << (bool)rrc.Get("is_done").As<Napi::Boolean>() <<std::endl;
    std::cout.flush();

    rrc.Set("is_done", true);

    std::cout << "info0.is_done = " << (bool)rrc.Get("is_done").As<Napi::Boolean>() <<std::endl;
    std::cout.flush();


    return info.Env().Null();
}

static void Range_Request_Thread_Safe(napi_env env, napi_value js_cb, void *context, void *data){
    RangeWorker *range_worker = (RangeWorker *)data;
    std::cout << "Range_Request_Thread_Safe length = " << range_worker->_length << std::endl;
    std::cout.flush();

    napi_ref range_request_context_ref = (napi_ref)context;
    napi_value range_request_context;
    napi_status status = napi_get_reference_value(env, range_request_context_ref, &range_request_context);
    
    std::cout << "Range_Request_Thread_Safe ns get ref = " << status << std::endl;
    std::cout.flush();

    napi_value start_nv, length_nv, buff_nv, res_nv;
    status = napi_create_int64(env, range_worker->_start, &start_nv); //TODO - check status
    std::cout << "Range_Request_Thread_Safe status1 = " << status << "buffer = " << range_worker->_buffer << std::endl;
    std::cout.flush();
    status = napi_create_int64(env, range_worker->_length, &length_nv);
    std::cout << "Range_Request_Thread_Safe status2 = " << status << std::endl;
    std::cout.flush();
    status = napi_create_buffer(env, range_worker->_length, &range_worker->_buffer, &buff_nv); //TODO - buffer should be a different pointer
    std::cout << "Range_Request_Thread_Safe status3 = " << status << "buffer = " << range_worker->_buffer << std::endl;
    std::cout.flush();
    std::cout << "Range_Request_Thread_Safe create val" << std::endl;
    std::cout.flush();
    napi_value argv[] = {start_nv, length_nv, buff_nv};
    status = napi_call_function(env, range_request_context, js_cb, 3, argv, &res_nv);
    std::cout << "range req ended = " << status << std::endl;
    std::cout.flush();
    std::cout << "range req res = " << res_nv << std::endl;
    std::cout.flush();
    Napi::Value res(env, res_nv);
    std::cout << "range req res IsPromise = " << res.IsPromise() << std::endl;
    std::cout.flush();
    Napi::Promise promise = res.As<Napi::Promise>();

    Napi::Function then = promise.Get("then").As<Napi::Function>();
    Napi::Function callback = Napi::Function::New(env, Range_Request_Thread_Safe_CB, "Range_Request_Thread_Safe_CB");
    then.Call(promise, {callback});
}

static void Is_Done_Thread_Safe(napi_env env, napi_value js_cb, void *context, void *data){
    RangeWorker *range_worker = (RangeWorker *)data;
    std::cout << "Range_Request_Thread_Safe length = " << range_worker->_length << std::endl;
    std::cout.flush();

    napi_ref range_request_context_ref = (napi_ref)context;
    napi_value range_request_context;
    napi_status status = napi_get_reference_value(env, range_request_context_ref, &range_request_context);
    
    std::cout << "Range_Request_Thread_Safe ns get ref = " << status << std::endl;
    std::cout.flush();

    Napi::Value rrc_nv(env, range_request_context);
    Napi::Object rrc = rrc_nv.As<Napi::Object>();
    bool _is_done = rrc.Get("is_done").As<Napi::Boolean>();
    //TODO - log, update range_worker.
}

class ParquetInitWorker : public Napi::AsyncWorker{
public:

    const uint32_t _size_bytes;
    //const Napi::Object &_range_request_context;
    s3selectEngine::parquet_object **_parquet_object;
    s3selectEngine::s3select *_s3select;
    Napi::Promise::Deferred _deferred;
    napi_threadsafe_function _range_request_napi;
    RangeWorker _range_worker;//(_env, start, length, buff, _range_request_napi);
    //napi_ref _rrc_nf;

    ParquetInitWorker(const Napi::CallbackInfo& info,
        const uint32_t size_bytes,
        s3selectEngine::parquet_object **parquet_object,
        s3selectEngine::s3select *s3select,
        napi_threadsafe_function &range_request_napi,
        napi_threadsafe_function &is_done_napi,
        napi_ref rrc_nf)
        : AsyncWorker(info.Env())
        , _range_worker(info, range_request_napi, is_done_napi, rrc_nf)
        , _size_bytes(size_bytes)
        , _parquet_object(parquet_object)
        , _s3select(s3select)
        , _range_request_napi(range_request_napi)
        , _deferred(info.Env())
        {}
    
    void Execute() override {

        s3selectEngine::rgw_s3select_api rgw;
        
        std::function<int(void)> fp_get_size=[&](){
            std::cout << "returning size_bytes = " << _size_bytes << std::endl;
            std::cout.flush();
            return _size_bytes;
        };
        rgw.set_get_size_api(fp_get_size);

        std::cout << "piwe4" << std::endl;
        std::cout.flush();

        std::cout << "piwe5" << std::endl;
        std::cout.flush();
        std::function<size_t(int64_t, int64_t, void *, optional_yield *)> fp_range_request = 
        [&](int64_t start, int64_t length, void *buff, optional_yield *y) {

            std::cout << "piwe range req start = " << start << " ,length = " << length << std::endl;
            std::cout.flush();
           
            _range_worker._start = start;
            _range_worker._length = length;
            _range_worker._buffer = buff;

            std::cout << "range req created = " << start << " ,length = " << length << std::endl;
            std::cout.flush();
            _range_worker.Queue();

            std::cout << "range req queued = " << start << " ,length = " << length << std::endl;
            std::cout.flush();
            while(!_range_worker._is_done){
                sleep(100);
                std::cout << "piwe is_done = " << _range_worker._is_done << std::endl;
                std::cout.flush();
            }
            return _range_worker._res;
        };

        rgw.set_range_req_api(fp_range_request);

        std::function<int(std::string&)> fp_s3select_result_format = [](std::string& result){std::cout << result;result.clear();return 0;};
        std::function<int(std::string&)> fp_s3select_header_format = [](std::string& result){result="";return 0;};
        /*std::function<void(const char*)> fp_debug = [](const char* msg)
        {
	        std::cout << "DEBUG: {" <<  msg << "}" << std::endl;
        };*/

        //s3selectEngine::parquet_object parquet_processor("/no/such/file", &s3select, &rgw);
        std::cout << "piwe6" << std::endl;
        std::cout.flush();
        *_parquet_object = new s3selectEngine::parquet_object("/no/such/file", _s3select, &rgw); //TODO dtor
        std::cout << "piwe7" << std::endl;
        std::cout.flush();

    }

    void OnOK() override {
        _deferred.Resolve(Env().Null());
    }

    void OnError(Napi::Error const& error) override {
        _deferred.Reject(Env().Null());
    }
};

class S3SelectNapi : public Napi::ObjectWrap<S3SelectNapi>
{

public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    S3SelectNapi(const Napi::CallbackInfo& info);
    Napi::Value Write(const Napi::CallbackInfo& info);
    Napi::Value Flush(const Napi::CallbackInfo& info);
    Napi::Value Select_Parquet_Start(const Napi::CallbackInfo& info);
    Napi::Value Select_Parquet(const Napi::CallbackInfo& info);
    size_t Range_Request(int64_t start, int64_t length, void *buff, optional_yield *y);
    ~S3SelectNapi();

private:
    static Napi::FunctionReference constructor;
    Napi::ObjectReference _args_ref;
    std::string input_format;
    s3selectEngine::s3select s3select;
    s3selectEngine::csv_object *csv_object = nullptr;
    s3selectEngine::json_object *json_object = nullptr;
    s3selectEngine::parquet_object *parquet_object = nullptr;
    const uint8_t* headers_buf;
    uint32_t headers_len;
    napi_value range_request_js;
    uint32_t size_bytes;
    napi_threadsafe_function range_request_napi;
    napi_threadsafe_function is_done_napi;
    napi_ref rrc_nf;
};

Napi::Value
S3SelectNapi::Select_Parquet_Start(const Napi::CallbackInfo& info)
{
    std::cout << "Select_Parquet_Start" << std::endl;
    std::cout.flush();

    Napi::Object context = info[0].As<Napi::Object>();
    size_bytes = context.Get("size_bytes").As<Napi::Number>().Uint32Value();
    Napi::Object range_request_context = context.Get("range_request_context").As<Napi::Object>();
    _args_ref.Set("range_request_context", range_request_context);
    napi_value range_request_context_nv = range_request_context;
    napi_create_reference(info.Env(), range_request_context_nv, 2, &rrc_nf);

    napi_status ns;
    ns = napi_get_named_property(info.Env(), range_request_context_nv, "range_request", &range_request_js);
    std::cout << "ns for get named prop = " << ns << std::endl;
    std::cout.flush();

    napi_value work_name;
    ns = napi_create_string_utf8(info.Env(),
            "Thread-safe Range Request Work Item",
            NAPI_AUTO_LENGTH,
            &work_name);
    std::cout << "ns for create string = " << ns << std::endl;
    std::cout.flush();

    std::cout << "&range_request_context_nv" << &range_request_context_nv << std::endl;
    std::cout.flush();
    std::cout << "range_request_context_nv" << range_request_context_nv << std::endl;
    std::cout.flush();

    ns = napi_create_threadsafe_function(
        info.Env(),
        range_request_js,
        nullptr,
        work_name,
        0,
        1,
        nullptr,
        nullptr,
        rrc_nf, /*TODO - context?*/
        Range_Request_Thread_Safe,
        &range_request_napi);
    std::cout << "ns for create tsfn = " << ns << std::endl;
    std::cout.flush();

    ns = napi_create_threadsafe_function(
        info.Env(),
        nullptr,
        nullptr,
        nullptr,
        0,
        1,
        nullptr,
        nullptr,
        rrc_nf, /*TODO - context?*/
        Is_Done_Thread_Safe,
        &is_done_napi);
    std::cout << "ns for create tsfn = " << ns << std::endl;
    std::cout.flush();


    ParquetInitWorker *parquet_init_worker = new ParquetInitWorker(
        info,
        size_bytes,
        &parquet_object,
        &s3select,
        range_request_napi,
        is_done_napi,
        rrc_nf);
    parquet_init_worker->Queue();
    return parquet_init_worker->_deferred.Promise();
}

Napi::Value
S3SelectNapi::Select_Parquet(const Napi::CallbackInfo& info) //TODO
{
    int status;
    std::string result;

    std::function<int(std::string&)> fp_s3select_result_format = [](std::string& result){std::cout << result;result.clear();return 0;};
    std::function<int(std::string&)> fp_s3select_header_format = [](std::string& result){result="";return 0;};

    do {
        try {
            status = parquet_object->run_s3select_on_object(result,fp_s3select_result_format,fp_s3select_header_format);
        }
        catch (s3selectEngine::base_s3select_exception &e) {
            std::cout << e.what() << std::endl;
            //m_error_description = e.what();
            //m_error_count++;
            if (e.severity() == s3selectEngine::base_s3select_exception::s3select_exp_en_t::FATAL){ //abort query execution
                return Napi::String();
            }
        }

        if(status<0)
        {
        std::cout << parquet_object->get_error_description() << std::endl;
        break;
        }

        std::cout << "select_parquet res = " << result << std::endl;
        std::cout.flush();
        return Napi::String::New(info.Env(), result);
        //std::cout << result << std::endl;

        /*if(status == 2) // limit reached
        {
            break;
        }*/

  } while (0);

  return Napi::String();
}

Napi::Value
S3SelectNapi::Write(const Napi::CallbackInfo& info)
{
    Napi::Buffer<char> buffer = info[0].As<Napi::Buffer<char>>();
    SelectWorker *worker = new SelectWorker(
        info,
        *this,
        csv_object,
        json_object,
        buffer.Data(),
        buffer.Length(),
        false,
        input_format,
        headers_buf,
        headers_len);
    worker->Queue();
    return worker->_deferred.Promise();
}

Napi::Value
S3SelectNapi::Flush(const Napi::CallbackInfo& info)
{
    SelectWorker *worker = new SelectWorker(
        info,
        *this,
        csv_object,
        json_object,
        nullptr, /*No buffer for flush*/
        0,
        true,
        input_format,
        headers_buf,
        headers_len);
    worker->Queue();
    return worker->_deferred.Promise();
}

Napi::FunctionReference S3SelectNapi::constructor;

Napi::Object
S3SelectNapi::Init(Napi::Env env, Napi::Object exports)
{
    Napi::HandleScope scope(env);

    Napi::Function func = DefineClass(
        env,
        "S3SelectNapi",
        {
            InstanceMethod("write", &S3SelectNapi::Write),
            InstanceMethod("flush", &S3SelectNapi::Flush),
            InstanceMethod("select_parquet_start", &S3SelectNapi::Select_Parquet_Start),
            InstanceMethod("select_parquet", &S3SelectNapi::Select_Parquet)
        }
    ); //end of DefineClass

    constructor = Napi::Persistent(func);
    constructor.SuppressDestruct();

    exports.Set("S3Select", func);
    return exports;
}

void
s3select_napi(Napi::Env env, Napi::Object exports)
{
    S3SelectNapi::Init(env, exports);
}

std::string
GetStringWithDefault(Napi::Object obj, std::string name, std::string dfault)
{
    if (obj.Has(name)) {
        return obj.Get(name).ToString().Utf8Value();
    }
    return dfault;
}

S3SelectNapi::S3SelectNapi(const Napi::CallbackInfo& info)
    : Napi::ObjectWrap<S3SelectNapi>(info)
    , _args_ref(Napi::Persistent(Napi::Object::New(info.Env())))
{

    std::cout << "ctor1" << std::endl;
    std::cout.flush();

    Napi::Env env = info.Env();
    Napi::Object context = info[0].As<Napi::Object>();
    _args_ref.Set("context", context);
    Napi::Object input_serialization_format = context.Get("input_serialization_format").As<Napi::Object>();
    Napi::Buffer headers_buf_obj = context.Get("records_header_buf").As<Napi::Buffer<uint8_t>>();
    _args_ref.Set("headers_buf", headers_buf_obj);
    headers_buf = headers_buf_obj.Data();
    headers_len = headers_buf_obj.Length();
    std::string query = context.Get("query").ToString();
    input_format = context.Get("input_format").ToString();

    s3select.parse_query(query.c_str());
    if (!s3select.get_error_description().empty()) {
        throw Napi::Error::New(env, XSTR() << "s3select: parse_query failed " << s3select.get_error_description());
    }

    if (input_format == JSON_FORMAT) {
        json_object = new s3selectEngine::json_object(&s3select);
    } else if (input_format == CSV_FORMAT) {
        s3selectEngine::csv_object::csv_defintions csv_defs;
        csv_defs.row_delimiter = GetStringWithDefault(input_serialization_format, "RecordDelimiter", "\n").c_str()[0];
        csv_defs.column_delimiter = GetStringWithDefault(input_serialization_format, "FieldDelimiter", ",").c_str()[0];
        csv_defs.ignore_header_info = (0 == GetStringWithDefault(input_serialization_format, "FileHeaderInfo", "").compare("IGNORE"));
        csv_defs.use_header_info = (0 == GetStringWithDefault(input_serialization_format, "FileHeaderInfo", "").compare("USE"));
        csv_defs.quote_fields_always = false;
        csv_object = new s3selectEngine::csv_object(&s3select, csv_defs);
    } else if (input_format == PARQUET_FORMAT) {

        std::cout << "ctor2" << std::endl;
        std::cout.flush();
    } else {
        throw Napi::Error::New(env, XSTR() << "input_format must be either CSV, JSON or Parquet.");
    }
}

S3SelectNapi::~S3SelectNapi()
{
    if (nullptr != csv_object) {
        delete csv_object;
    }
    if (nullptr != json_object) {
        delete json_object;
    }
}

} // namespace noobaa
