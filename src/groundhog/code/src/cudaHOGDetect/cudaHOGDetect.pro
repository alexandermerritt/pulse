TEMPLATE = app
QT = gui core
CONFIG += debug console

HEADERS =
SOURCES = main.cpp

INCLUDEPATH += ../../cudaHOG
LIBS += -lboost_program_options -lcudaHOG -L../../cudaHOG -L/usr/local/cuda/lib64

DESTDIR = ../../bin
