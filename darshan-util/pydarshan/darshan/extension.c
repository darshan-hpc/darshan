/*
 * NOTE: The pydarshan C extension is used to allow for an automatic
 * build process to distribute binary wheels with a copy of pydarshan
 * with darshan-utils included. It currently provides no functionality
 * beyond this, but darshan-util functionality currently exposed using
 * CFFI could be also integrated using the extension.
 */


#include "Python.h"


static PyObject *
py_verify_magic(PyObject *self, PyObject *args)
{
    /* kept for debugging  */
    double magic = 305419896; /* 0x12345678 */
    return PyFloat_FromDouble(magic);
}

static PyMethodDef module_functions[] = {
	{"verify_magic",  py_verify_magic, METH_VARARGS, NULL},
	{NULL, NULL}          /* sentinel */
};


#if PY_MAJOR_VERSION >= 3
    static struct PyModuleDef moduledef = {
        PyModuleDef_HEAD_INIT,
        "extension",          /* m_name */
        "",                   /* m_doc */
        -1,                   /* m_size */
        module_functions,     /* m_methods */
        NULL,                 /* m_reload */
        NULL,                 /* m_traverse */
        NULL,                 /* m_clear */
        NULL,                 /* m_free */
    };
#endif


static PyObject *
moduleinit(void)
{
    PyObject *m;

#if PY_MAJOR_VERSION >= 3
    m = PyModule_Create(&moduledef);
#else
    m = Py_InitModule3("extension", module_functions, "Dependency Helper.");
#endif
  return m;
}

#if PY_MAJOR_VERSION < 3
    PyMODINIT_FUNC
    initextension(void)
    {
        moduleinit();
    }
#else
    PyMODINIT_FUNC
    PyInit_extension(void)
    {
        return moduleinit();
    }
#endif
