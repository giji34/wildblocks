#include <minecraft-file.hpp>
#include <iostream>
#include <sqlite3.h>
#include <sstream>
#include "hwm/task/task_queue.hpp"

using namespace std;
using namespace mcfile;

static void print_description() {
    cerr << "wildblocks" << endl;
    cerr << "SYNOPSYS" << endl;
    cerr << "    wildblocks [db file path] [world directory] [dimension] [minecraft version]" << endl;
    cerr << "DIMENSION" << endl;
    cerr << "    0:  Overworld" << endl;
    cerr << "    -1: The Nether" << endl;
    cerr << "    1:  The End" << endl;
    cerr << "MINECRAFT VERSION" << endl;
    cerr << "    1.13 etc." << endl;
}

static int queryMaterialsCallback(void *user, int argc, char **argv, char **columnName) {
    map<string, blocks::BlockId> *blocks = (map<string, blocks::BlockId>*)user;
    if (argc < 2) {
        return 0;
    }
    int id = -1;
    string name;

    for (int i = 0; i < argc; i++) {
        string column = columnName[i];
        string value = argv[i];
        if (column == "id") {
            int v;
            if (sscanf(argv[i], "%d", &v) == 1) {
                id = v;
            }
        } else if (column == "name") {
            name = argv[i];
        }
    }
    if (id > 0) {
        blocks->insert(make_pair(name, id));
    }
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc < 5) {
        print_description();
        return -1;
    }
    string dbFile = argv[1];
    string worldDir = argv[2];
    int dimension = 0;
    if (sscanf(argv[3], "%d", &dimension) != 1) {
        print_description();
        return -1;
    }
    string version = argv[4];
    cout << "db:        " << dbFile << endl;
    cout << "world:     " << worldDir << endl;
    cout << "dimension: " << dimension << endl;
    cout << "version:   " << version << endl;

    if (sqlite3_config(SQLITE_CONFIG_SERIALIZED) != SQLITE_OK) {
        cerr << "Can't select serialized mode" << endl;
        return -1;
    }
    
    sqlite3* db = nullptr;
    if (sqlite3_open(dbFile.c_str(), &db)) {
        cerr << "Can't open database: " << dbFile << endl;
        sqlite3_close(db);
        return -1;
    }

    char *error = nullptr;
    string createMaterialsTable = R"(
        create table if not exists materials (
            id integer unique not null,
            name string unique not null
        );)";
    if (sqlite3_exec(db, createMaterialsTable.c_str(), nullptr, nullptr, &error) != SQLITE_OK) {
        cerr << error << endl;
        sqlite3_free(error);
        sqlite3_close(db);
        return -1;
    }

    string createWildBlocksTable = R"(
        create table if not exists wild_blocks (
            x integer not null,
            y integer not null,
            z integer not null,
            dimension integer not null,
            version text not null,
            material_id integer,
            data text
        );)";
    if (sqlite3_exec(db, createWildBlocksTable.c_str(), nullptr, nullptr, &error) != SQLITE_OK) {
        cerr << error << endl;
        sqlite3_free(error);
        sqlite3_close(db);
        return -1;
    }

    map<string, blocks::BlockId> blocks;
    string queryMaterials = "select * from materials;";
    if (sqlite3_exec(db, queryMaterials.c_str(), queryMaterialsCallback, &blocks, &error) != SQLITE_OK) {
        cerr << error << endl;
        sqlite3_free(error);
        sqlite3_close(db);
        return -1;
    }

    ostringstream insertToMaterials;
    insertToMaterials << "insert or replace into materials (id, name) values ";
    bool first = true;
    for (blocks::BlockId id = 1; id < blocks::minecraft::minecraft_max_block_id; id++) {
        string name = blocks::Name(id);
        if (blocks.find(name) != blocks.end()) {
            continue;
        }
        if (!first) {
            insertToMaterials << ", ";
        }
        first = false;
        insertToMaterials << "(" << id << ", \"" << name << "\")";
        blocks.insert(make_pair(name, id));
    }
    insertToMaterials << ";";
    if (!first) {
        if (sqlite3_exec(db, insertToMaterials.str().c_str(), nullptr, nullptr, &error) != SQLITE_OK) {
            cerr << error << endl;
            sqlite3_free(error);
            sqlite3_close(db);
            return -1;
        }
    }
    
    string createIndex = "create unique index if not exists wild_blocks_unique_coordinate on wild_blocks (x, y, z, dimension, version);";
    if (sqlite3_exec(db, createIndex.c_str(), nullptr, nullptr, &error) != SQLITE_OK) {
        cerr << error << endl;
        sqlite3_free(error);
        sqlite3_close(db);
        return -1;
    }

    hwm::task_queue q(thread::hardware_concurrency());
    vector<future<void>> futures;
    mutex m;

    World world(worldDir);
    world.eachRegions([db, dimension, version, &futures, &q, &m](shared_ptr<Region> const& region) {
        int regionX = region->fX;
        int regionZ = region->fZ;
        futures.emplace_back(q.enqueue([db, dimension, version, region, &m, regionX, regionZ]() {
            cout << "region [" << regionX << ", " << regionZ << "]" << endl;
            bool error = false;
            region->loadAllChunks(error, [db, dimension, version, &m](Chunk const& chunk) {
                bool first = true;
                ostringstream insert;
                insert << "insert or replace into wild_blocks (x, y, z, dimension, version, material_id, data) values ";
                for (int y = 0; y < 256; y++) {
                    for (int x = chunk.minBlockX(); x <= chunk.maxBlockX(); x++) {
                        for (int z = chunk.minBlockZ(); z <= chunk.maxBlockZ(); z++) {
                            shared_ptr<Block> block = chunk.blockAt(x, y, z);
                            if (!block) {
                                continue;
                            }
                            blocks::BlockId id = blocks::FromName(block->fName);
                            if (id == blocks::unknown) {
                                continue;
                            }
                            if (id == blocks::minecraft::air && block->fProperties.empty()) {
                                continue;
                            }
                            ostringstream values;
                            values << "(" << x << ", " << y << ", " << z << ", " << dimension << ", \"" << version << "\", " << id << ", ";
                            if (!block->fProperties.empty()) {
                                values << "\"{";
                                bool f = true;
                                for (auto it = block->fProperties.begin(); it != block->fProperties.end(); it++) {
                                    if (!f) {
                                        values << ",";
                                    }
                                    values << it->first + "=" + it->second;
                                    f = false;
                                }
                                values << "}\"";
                            } else {
                                values << "null";
                            }
                            values << ")";
                            if (!first) {
                                insert << ", ";
                            }
                            insert << values.str();
                            first = false;
                        }
                    }
                }
                insert << ";";
                if (!first) {
                    lock_guard<mutex> lock(m);
                    char *e = nullptr;
                    if (sqlite3_exec(db, insert.str().c_str(), nullptr, nullptr, &e) != SQLITE_OK) {
                        cerr << e << endl;
                        cerr << "query=" << insert.str() << endl;
                        sqlite3_free(e);
                    }
                }
                return true;
            });
        }));
    });

    for (auto& f : futures) {
        f.get();
    }

    sqlite3_close(db);
}
