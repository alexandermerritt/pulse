#include <iostream>
#include <string>
#include <vector>
#include <list>
#include <map>
#include <utility>
#include <sstream>

#include <string.h>
#include <stdio.h>

#include <linux/elf.h>

using namespace std;

map<size_t, string> regions;

// ./parsemaps [addr] < maps.[pid]
// search the maps file (created when you LD_PRELOAD=lib.so ./feature [args])
// for the given address to see which .so it belongs to
int main(int argc, char *argv[])
{
    size_t end;
    list<string> input;
    string line;

    if (argc != 2)
        return 1;

    size_t addr;
    if (1 != sscanf(argv[1], "0x%lx", &addr))
        return 1;

    // read in all lines
    while (getline(cin, line))
        input.push_back(line);

    // store into our index
    for (string &l : input) {
        line = l.substr(l.find('-')+1,l.find(' '));
        if (1 != sscanf(line.c_str(), "%lx", &end))
            return 1;
        regions.insert(make_pair(end, l));
    }

    // find address
    string entry;
    auto it = regions.upper_bound(addr);
    if (it != regions.end()) {
        size_t start;
        string &l = it->second;
        line = l.substr(0, l.find('-'));
        if (1 == sscanf(line.c_str(), "%lx", &start))
            if (start <= addr)
                entry = l;
    }
    if (entry.size() == 0)
        return 2;

    // lookup the symbol of the address
    string path(entry.substr(entry.rfind(' ')+1));
    cout << "opening " << path << endl;
    FILE *fp = fopen(path.c_str(), "r");
    if (!fp)
        return 3;

    fclose(fp);
    return 0;
}
