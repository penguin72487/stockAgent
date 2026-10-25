// Coarse-grained walk of borrowed, already-decoded JSON objects.
// Classification and finite-number semantics remain Python-owned callbacks.
#define PY_SSIZE_T_CLEAN
#include <Python.h>

struct Ref {
    PyObject *p;
    explicit Ref(PyObject *value) : p(value) {}
    ~Ref() { Py_XDECREF(p); }
    Ref(const Ref &) = delete;
    PyObject *release() { PyObject *value = p; p = nullptr; return value; }
};

// Each cache is bound to one pure Python key classifier, never to JSON values.
// A native hit avoids entering Python; misses retain the canonical callback.
struct DescriptorCache {
    PyObject_HEAD
    PyObject *callback, *entries;
    unsigned long long hits, misses;
};

static PyTypeObject DescriptorCacheType = {PyVarObject_HEAD_INIT(nullptr, 0)};

static int descriptor_clear(DescriptorCache *self) {
    Py_CLEAR(self->callback);
    Py_CLEAR(self->entries);
    return 0;
}

static int descriptor_traverse(DescriptorCache *self, visitproc visit, void *arg) {
    Py_VISIT(self->callback);
    Py_VISIT(self->entries);
    return 0;
}

static void descriptor_dealloc(DescriptorCache *self) {
    PyObject_GC_UnTrack(self);
    descriptor_clear(self);
    PyObject_GC_Del(self);
}

static PyObject *descriptor_lookup(DescriptorCache *self, PyObject *key) {
    if (!self->callback || !self->entries) {
        PyErr_SetString(PyExc_RuntimeError, "cleared metric descriptor cache");
        return nullptr;
    }
    bool cacheable = PyUnicode_CheckExact(key) && PyUnicode_GET_LENGTH(key) <= 256;
    if (cacheable) {
        PyObject *hit = PyDict_GetItemWithError(self->entries, key);
        if (hit) {
            ++self->hits;
            return Py_NewRef(hit);
        }
        if (PyErr_Occurred()) return nullptr;
    }
    ++self->misses;
    Ref meta(PyObject_CallOneArg(self->callback, key));
    if (!meta.p) return nullptr;
    // Unusual callback results keep the existing walker ABI/error behavior,
    // but only immutable (bool, bool, str) semantics may be retained.
    if (cacheable && PyTuple_CheckExact(meta.p) && PyTuple_GET_SIZE(meta.p) == 3
        && PyBool_Check(PyTuple_GET_ITEM(meta.p, 0))
        && PyBool_Check(PyTuple_GET_ITEM(meta.p, 1))
        && PyUnicode_CheckExact(PyTuple_GET_ITEM(meta.p, 2))) {
        if (PyDict_GET_SIZE(self->entries) >= 4096) PyDict_Clear(self->entries);
        if (PyDict_SetItem(self->entries, key, meta.p) < 0) return nullptr;
    }
    return meta.release();
}

static PyObject *descriptor_call(PyObject *object, PyObject *args, PyObject *kwargs) {
    PyObject *key;
    if (kwargs && PyDict_GET_SIZE(kwargs)) {
        PyErr_SetString(PyExc_TypeError, "metric descriptor accepts one positional key");
        return nullptr;
    }
    if (!PyArg_ParseTuple(args, "O:metric_descriptor", &key)) return nullptr;
    return descriptor_lookup(reinterpret_cast<DescriptorCache *>(object), key);
}

static PyObject *descriptor_info(DescriptorCache *self, PyObject *) {
    return Py_BuildValue("{s:n,s:i,s:i,s:K,s:K}", "size",
        self->entries ? PyDict_GET_SIZE(self->entries) : 0,
        "maxsize", 4096, "max_key_characters", 256,
        "hits", self->hits, "misses", self->misses);
}

static PyMethodDef descriptor_methods[] = {
    {"cache_info", reinterpret_cast<PyCFunction>(descriptor_info), METH_NOARGS,
     "Report bounded key semantics only; eviction clears entries at capacity."},
    {nullptr, nullptr, 0, nullptr}
};

static PyObject *cache_descriptor(PyObject *, PyObject *callback) {
    if (!PyCallable_Check(callback)) {
        PyErr_SetString(PyExc_TypeError, "key descriptor must be callable");
        return nullptr;
    }
    Ref entries(PyDict_New());
    if (!entries.p) return nullptr;
    auto *self = PyObject_GC_New(DescriptorCache, &DescriptorCacheType);
    if (!self) return nullptr;
    self->callback = Py_NewRef(callback);
    self->entries = entries.release();
    self->hits = self->misses = 0;
    PyObject_GC_Track(self);
    return reinterpret_cast<PyObject *>(self);
}

struct Walker {
    PyObject *descriptor, *finite, *output, *slash, *ms, *seconds, *milliseconds, *semantic;
    PyObject *field_pointer, *field_value, *field_unit, *field_semantic;

    PyObject *path(PyObject *prefix, PyObject *part) {
        Ref start(PyUnicode_Concat(prefix, slash));
        return start.p ? PyUnicode_Concat(start.p, part) : nullptr;
    }

    int unsupported() {
        PyErr_SetString(PyExc_NotImplementedError, "non-JSON container/key or depth exceeds native scope");
        return -1;
    }

    int walk(PyObject *value, PyObject *pointer, int depth) {
        if (depth >= 64) return unsupported();
        if (!PyDict_Check(value) && !PyList_Check(value)) return 0;
        if (!PyDict_CheckExact(value) && !PyList_CheckExact(value)) return unsupported();
        if (Py_EnterRecursiveCall(" while walking timing evidence")) return -1;
        int result = walk_container(value, pointer, depth);
        Py_LeaveRecursiveCall();
        return result;
    }

    int walk_container(PyObject *value, PyObject *pointer, int depth) {
        if (PyList_CheckExact(value)) {
            for (Py_ssize_t i = 0; i < PyList_GET_SIZE(value); ++i) {
                PyObject *child = PyList_GET_ITEM(value, i); // borrowed; GIL held
                if (!PyDict_Check(child) && !PyList_Check(child)) continue;
                Ref index(PyUnicode_FromFormat("%zd", i));
                if (!index.p) return -1;
                Ref next(path(pointer, index.p));
                if (!next.p || walk(child, next.p, depth + 1) < 0) return -1;
            }
            return 0;
        }
        Py_ssize_t position = 0;
        PyObject *key, *child;
        while (PyDict_Next(value, &position, &key, &child)) {
            if (!PyUnicode_CheckExact(key)) return unsupported();
            Ref meta(Py_IS_TYPE(descriptor, &DescriptorCacheType)
                     ? descriptor_lookup(reinterpret_cast<DescriptorCache *>(descriptor), key)
                     : PyObject_CallOneArg(descriptor, key));
            if (!meta.p) return -1;
            if (!PyTuple_CheckExact(meta.p) || PyTuple_GET_SIZE(meta.p) != 3) {
                PyErr_SetString(PyExc_TypeError, "invalid key descriptor ABI");
                return -1;
            }
            int visible = PyObject_IsTrue(PyTuple_GET_ITEM(meta.p, 0));
            int timing = PyObject_IsTrue(PyTuple_GET_ITEM(meta.p, 1));
            if (visible < 0 || timing < 0) return -1;
            if (PyDict_Check(child) || PyList_Check(child)) {
                if (!visible) continue;
                Ref next(path(pointer, PyTuple_GET_ITEM(meta.p, 2)));
                if (!next.p || walk(child, next.p, depth + 1) < 0) return -1;
            } else if (timing) {
                Ref valid(PyObject_CallOneArg(finite, child));
                if (!valid.p) return -1;
                int is_valid = PyObject_IsTrue(valid.p);
                if (is_valid < 0) return -1;
                if (!is_valid) continue;
                Ref next(path(pointer, PyTuple_GET_ITEM(meta.p, 2)));
                if (!next.p) return -1;
                Py_ssize_t ends_ms = PyUnicode_Tailmatch(key, ms, 0, PY_SSIZE_T_MAX, 1);
                if (ends_ms < 0) return -1;
                Ref row(Py_BuildValue("{O:O,O:O,O:O,O:O}", field_pointer, next.p,
                    field_value, child, field_unit, ends_ms ? milliseconds : seconds,
                    field_semantic, semantic));
                if (!row.p || PyList_Append(output, row.p) < 0) return -1;
            }
        }
        return 0;
    }
};

static PyObject *rows(PyObject *, PyObject *args) {
    PyObject *value, *pointer, *descriptor, *finite;
    if (!PyArg_ParseTuple(args, "OO!OO:rows", &value, &PyUnicode_Type, &pointer, &descriptor, &finite)) return nullptr;
    Ref output(PyList_New(0)), slash(PyUnicode_FromString("/")), ms(PyUnicode_FromString("_ms"));
    Ref seconds(PyUnicode_FromString("seconds")), milliseconds(PyUnicode_FromString("milliseconds"));
    Ref semantic(PyUnicode_FromString("producer_reported_requires_scope_review"));
    Ref field_pointer(PyUnicode_InternFromString("pointer")), field_value(PyUnicode_InternFromString("reported_value"));
    Ref field_unit(PyUnicode_InternFromString("reported_unit")), field_semantic(PyUnicode_InternFromString("semantic_state"));
    if (!output.p || !slash.p || !ms.p || !seconds.p || !milliseconds.p || !semantic.p
        || !field_pointer.p || !field_value.p || !field_unit.p || !field_semantic.p) return nullptr;
    Walker walker{descriptor, finite, output.p, slash.p, ms.p, seconds.p, milliseconds.p, semantic.p,
                  field_pointer.p, field_value.p, field_unit.p, field_semantic.p};
    if (walker.walk(value, pointer, 0) < 0) return nullptr;
    return output.release();
}


#ifdef __linux__
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <new>
#include <string>
#include <vector>

struct Row {
    std::string path, raw;
    struct stat before{}, after{};
    bool skip = false, ready = false;
};

static PyObject *signature(const struct stat &s) {
    return Py_BuildValue("(KKLLL)", (unsigned long long)s.st_dev,
        (unsigned long long)s.st_ino, (long long)s.st_size,
        (long long)s.st_mtim.tv_sec * 1000000000LL + s.st_mtim.tv_nsec,
        (long long)s.st_ctim.tv_sec * 1000000000LL + s.st_ctim.tv_nsec);
}

static bool batch_times_fit(const struct stat &s) {
    constexpr long long limit = 9223372035LL;
    return s.st_mtim.tv_sec >= -limit && s.st_mtim.tv_sec <= limit
        && s.st_ctim.tv_sec >= -limit && s.st_ctim.tv_sec <= limit;
}

static void prepare(Row &row, Py_ssize_t &remaining, Py_ssize_t maximum,
                    Py_ssize_t chunk) {
    if (row.skip || stat(row.path.c_str(), &row.before) != 0) return;
    const auto size = row.before.st_size;
    if (!S_ISREG(row.before.st_mode) || size < 0 || size > remaining || size > maximum
        || !batch_times_fit(row.before)) return;
    int fd;
    do { fd = open(row.path.c_str(), O_RDONLY | O_CLOEXEC | O_NONBLOCK); }
    while (fd < 0 && errno == EINTR);
    if (fd < 0) return;
    // A path may change between stat and open; never read a replaced FIFO/device.
    struct stat opened{};
    if (fstat(fd, &opened) != 0 || !S_ISREG(opened.st_mode)) { close(fd); return; }
    try { row.raw.resize((size_t)size + 1); }
    catch (...) { close(fd); throw; }
    size_t total = 0;
    bool failed = false;
    do {
        size_t requested = (size_t)size + 1 - total;
        if (chunk && requested > (size_t)chunk) requested = (size_t)chunk;
        ssize_t count;
        do { count = read(fd, row.raw.data() + total, requested); }
        while (count < 0 && errno == EINTR);
        if (count < 0) { failed = true; break; }
        total += (size_t)count;
        if (!count || total >= (size_t)size) break;
    } while (total < (size_t)size);
    // Do not retry close(EINTR): Linux has already released the descriptor.
    close(fd);
    if (failed || stat(row.path.c_str(), &row.after) != 0 || !batch_times_fit(row.after)) {
        std::string().swap(row.raw);
        return;
    }
    row.raw.resize(total);
    remaining -= (Py_ssize_t)total;
    row.ready = true;
}

static PyObject *read_batch(PyObject *, PyObject *args) {
    PyObject *paths;
    Py_ssize_t budget, maximum, chunk = 0;
    if (!PyArg_ParseTuple(args, "Onn|n:read_batch", &paths, &budget, &maximum, &chunk)) return nullptr;
    if (!PyList_CheckExact(paths) || PyList_GET_SIZE(paths) > 1024 || budget < 0
        || budget > (256 << 20) || maximum < 0 || chunk < 0) {
        PyErr_SetString(PyExc_ValueError, "private batch bounds exceeded"); return nullptr;
    }
    std::vector<Row> rows;
    try {
        rows.resize((size_t)PyList_GET_SIZE(paths));
        for (size_t i = 0; i < rows.size(); ++i) {
            PyObject *path = PyList_GET_ITEM(paths, (Py_ssize_t)i);
            if (path == Py_None) { rows[i].skip = true; continue; }
            if (!PyBytes_CheckExact(path)) {
                PyErr_SetString(PyExc_TypeError, "paths must be exact bytes or None"); return nullptr;
            }
            const char *data = PyBytes_AS_STRING(path);
            const auto length = PyBytes_GET_SIZE(path);
            if (memchr(data, 0, (size_t)length)) {
                PyErr_SetString(PyExc_ValueError, "embedded null byte"); return nullptr;
            }
            rows[i].path.assign(data, (size_t)length);
        }
    } catch (const std::bad_alloc &) { return PyErr_NoMemory(); }
    bool exhausted = false, unexpected = false;
    Py_BEGIN_ALLOW_THREADS
    try {
        for (auto &row : rows) prepare(row, budget, maximum, chunk);
    } catch (const std::bad_alloc &) { exhausted = true; }
      catch (...) { unexpected = true; }
    Py_END_ALLOW_THREADS
    if (exhausted) return PyErr_NoMemory();
    if (unexpected) { PyErr_SetString(PyExc_RuntimeError, "native batch failure"); return nullptr; }
    PyObject *output = PyList_New((Py_ssize_t)rows.size());
    if (!output) return nullptr;
    for (size_t i = 0; i < rows.size(); ++i) {
        auto &row = rows[i];
        PyObject *value = Py_NewRef(Py_None);
        if (row.ready) {
            Py_DECREF(value);
            PyObject *before = signature(row.before), *after = signature(row.after);
            PyObject *raw = PyBytes_FromStringAndSize(row.raw.data(), (Py_ssize_t)row.raw.size());
            if (!before || !after || !raw) {
                Py_XDECREF(before); Py_XDECREF(after); Py_XDECREF(raw); Py_DECREF(output); return nullptr;
            }
            value = PyTuple_Pack(3, before, after, raw);
            Py_DECREF(before); Py_DECREF(after); Py_DECREF(raw);
            if (!value) { Py_DECREF(output); return nullptr; }
            std::string().swap(row.raw);
        }
        PyList_SET_ITEM(output, (Py_ssize_t)i, value);
    }
    return output;
}

#endif

static PyMethodDef methods[] = {
#ifdef __linux__
    {"read_batch", read_batch, METH_VARARGS, "Ordered bounded stat/read/stat; consumer owns verification."},
#endif
    {"rows", rows, METH_VARARGS, "Walk decoded plain JSON without converting numeric values."},
    {"cache_descriptor", cache_descriptor, METH_O,
     "Bind one pure key classifier to a bounded native semantics cache."},
    {nullptr, nullptr, 0, nullptr}
};
static PyModuleDef module = {PyModuleDef_HEAD_INIT, "_workflow_metric_tree", nullptr, -1, methods};
PyMODINIT_FUNC PyInit__workflow_metric_tree() {
    DescriptorCacheType.tp_name = "_workflow_metric_tree.DescriptorCache";
    DescriptorCacheType.tp_basicsize = sizeof(DescriptorCache);
    DescriptorCacheType.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC;
    DescriptorCacheType.tp_dealloc = reinterpret_cast<destructor>(descriptor_dealloc);
    DescriptorCacheType.tp_traverse = reinterpret_cast<traverseproc>(descriptor_traverse);
    DescriptorCacheType.tp_clear = reinterpret_cast<inquiry>(descriptor_clear);
    DescriptorCacheType.tp_call = descriptor_call;
    DescriptorCacheType.tp_methods = descriptor_methods;
    if (PyType_Ready(&DescriptorCacheType) < 0) return nullptr;
    return PyModule_Create(&module);
}
