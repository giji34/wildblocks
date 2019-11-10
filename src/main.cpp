#include <minecraft-file.hpp>
#include <iostream>

using namespace std;

static void print_description() {
    printf("wildblocks\n");
    printf("SYNOPSYS\n");
    printf("    wildblocks [db file path] [region directory] [dimension] [minecraft version]\n");
    printf("DIMENSION\n");
    printf("    0:  Overworld\n");
    printf("    -1: The Nether\n");
    printf("    1:  The End\n");
    printf("MINECRAFT VERSION\n");
    printf("    1.13 etc.\n");
}

int main(int argc, char *argv[]) {
    if (argc < 5) {
        print_description();
        return -1;
    }
    string dbFile = argv[1];
    string regionDir = argv[2];
    int dimension = 0;
    if (sscanf(argv[3], "%d", &dimension) != 1) {
        print_description();
        return -1;
    }
    string version = argv[4];
    cout << "db:        " << dbFile << endl;
    cout << "region:    " << regionDir << endl;
    cout << "dimension: " << dimension << endl;
    cout << "version:   " << version << endl;
}
