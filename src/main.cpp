#include <minecraft-file.hpp>
#include <iostream>
#include <sqlite3.h>
#include <sstream>
#include <set>
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
    map<string, int> *blocks = (map<string, int>*)user;
    if (argc < 2) {
        return 0;
    }
    int id = -1;
    string data;

    for (int i = 0; i < argc; i++) {
        string column = columnName[i];
        string value = argv[i];
        if (column == "id") {
            int v;
            if (sscanf(argv[i], "%d", &v) == 1) {
                id = v;
            }
        } else if (column == "data") {
            data = argv[i];
        }
    }
    if (id > 0 && blocks->find(data) == blocks->end()) {
        cout << "read:[" << id << "]" << data << endl;
        blocks->insert(make_pair(data, id));
    }
    return 0;
}

static int queryExistingChunksCallback(void *user, int argc, char **argv, char **columnName) {
    set<pair<int, int>> *v = (set<pair<int, int>> *)user;
    int chunkX, chunkZ, count;
    int num = 0;
    for (int i = 0; i < argc; i++) {
        string name(columnName[i]);
        int v;
        if (sscanf(argv[i], "%d", &v) != 1) {
            continue;
        }
        if (name == "count(*)") {
            count = v;
        } else if (name == "chunkX") {
            chunkX = v;
        } else if (name == "chunkZ") {
            chunkZ = v;
        } else {
            continue;
        }
        num++;
    }
    if (num == 3 && count > 0) {
        v->insert(make_pair(chunkX, chunkZ));
    }
    return 0;
}

// minecraft:grass_block[snowy=false]
static string getBlockData(shared_ptr<Block> const& block) {
    ostringstream s;
    s << block->fName;
    if (!block->fProperties.empty()) {
        s << "[";
        bool f = true;
        for (auto it = block->fProperties.begin(); it != block->fProperties.end(); it++) {
            if (!f) {
                s << ",";
            }
            s << it->first + "=" + it->second;
            f = false;
        }
        s << "]";
    }
    return s.str();
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
            id integer primary key autoincrement,
            data string unique not null
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
            material_id integer
        );)";
    if (sqlite3_exec(db, createWildBlocksTable.c_str(), nullptr, nullptr, &error) != SQLITE_OK) {
        cerr << error << endl;
        sqlite3_free(error);
        sqlite3_close(db);
        return -1;
    }

    map<string, int> blockLut;
    mutex lutMutex;
    string queryMaterials = "select * from materials;";
    if (sqlite3_exec(db, queryMaterials.c_str(), queryMaterialsCallback, &blockLut, &error) != SQLITE_OK) {
        cerr << error << endl;
        sqlite3_free(error);
        sqlite3_close(db);
        return -1;
    }
    
    string createIndex = "create unique index if not exists wild_blocks_unique_coordinate on wild_blocks (x, y, z, dimension, version);";
    if (sqlite3_exec(db, createIndex.c_str(), nullptr, nullptr, &error) != SQLITE_OK) {
        cerr << error << endl;
        sqlite3_free(error);
        sqlite3_close(db);
        return -1;
    }

    set<pair<int, int>> existingChunks;
    if (sqlite3_exec(db, "select count(*), x >> 4 as chunkX, z >> 4 as chunkZ from wild_blocks group by chunkX, chunkZ;", queryExistingChunksCallback, &existingChunks, &error) != SQLITE_OK) {
        cerr << error << endl;
        sqlite3_free(error);
        sqlite3_close(db);
        return -1;
    }
    
    hwm::task_queue q(thread::hardware_concurrency());
    vector<future<void>> futures;
    mutex dbMutex;

    World world(worldDir);
    world.eachRegions([db, version, dimension, &existingChunks, &futures, &q, &blockLut, &dbMutex, &lutMutex](shared_ptr<Region> const& region) {
        for (int localChunkX = 0; localChunkX <= 32; localChunkX++) {
            for (int localChunkZ = 0; localChunkZ <= 32; localChunkZ++) {
                int chunkX = region->fX * 32 + localChunkX;
                int chunkZ = region->fZ * 32 + localChunkZ;
                if (existingChunks.find(make_pair(chunkX, chunkZ)) != existingChunks.end()) {
                    continue;
                }
                
                futures.emplace_back(q.enqueue([db, region, version, dimension, localChunkX, localChunkZ, &dbMutex, &lutMutex, &blockLut]() {
                    bool error = false;
                    region->loadChunk(localChunkX, localChunkZ, error, [=, &dbMutex, &lutMutex, &blockLut](Chunk const& chunk) {
                        bool first = true;
                        
                        map<tuple<int, int, int>, int> knownMaterials;
                        map<tuple<int, int, int>, string> unknownMaterials;
                        set<string> unknownMaterialNames;
                        {
                            lock_guard<mutex> lk(lutMutex);
                            for (int y = 0; y < 256; y++) {
                                for (int x = chunk.minBlockX(); x <= chunk.maxBlockX(); x++) {
                                    for (int z = chunk.minBlockZ(); z <= chunk.maxBlockZ(); z++) {
                                        shared_ptr<Block> block = chunk.blockAt(x, y, z);
                                        if (!block) {
                                            continue;
                                        }
                                        string blockData = getBlockData(block);
                                        if (blockData.empty()) {
                                            continue;
                                        }
                                        if (blockLut.find(blockData) == blockLut.end()) {
                                            unknownMaterials.insert(make_pair(make_tuple(x, y, z), blockData));
                                            unknownMaterialNames.insert(blockData);
                                        } else {
                                            int materialId = blockLut[blockData];
                                            knownMaterials.insert(make_pair(make_tuple(x, y, z), materialId));
                                        }
                                    }
                                }
                            }
                            if (!unknownMaterials.empty()) {
                                ostringstream ss;
                                ss << "insert into materials (data) values ";
                                bool f = true;
                                for (auto it = unknownMaterialNames.begin(); it != unknownMaterialNames.end(); it++) {
                                    string blockData = *it;
                                    if (!f) {
                                        ss << ", ";
                                    }
                                    f = false;
                                    ss << "(\"" << blockData << "\")";
                                }
                                ss << ";";
                                {
                                    lock_guard<mutex> lk(dbMutex);
                                    char *e = nullptr;
                                    if (sqlite3_exec(db, ss.str().c_str(), nullptr, nullptr, &e) != SQLITE_OK) {
                                        cerr << e << endl;
                                        sqlite3_free(e);
                                    }
                                    
                                    if (sqlite3_exec(db, "select * from materials;", queryMaterialsCallback, &blockLut, &e) != SQLITE_OK) {
                                        cerr << e << endl;
                                        sqlite3_free(e);
                                    }
                                }
                                for (auto it = unknownMaterials.begin(); it != unknownMaterials.end(); it++) {
                                    auto xyz = it->first;
                                    string blockData = it->second;
                                    auto found = blockLut.find(blockData);
                                    if (found == blockLut.end()) {
                                        cerr << "データ不整合" << endl;
                                        exit(-1);
                                    }
                                    knownMaterials.insert(make_pair(xyz, found->second));
                                }
                            }
                        }
                        map<tuple<int, int, int>, string>().swap(unknownMaterials);
                        set<string>().swap(unknownMaterialNames);
                        
                        ostringstream insert;
                        insert << "insert or replace into wild_blocks (x, y, z, dimension, version, material_id) values ";

                        for (auto it = knownMaterials.begin(); it != knownMaterials.end(); it++) {
                            auto xyz = it->first;
                            int materialId = it->second;
                            if (!first) {
                                insert << ", ";
                            }
                            insert << "(" << get<0>(xyz) << ", " << get<1>(xyz) << ", " << get<2>(xyz) << ", " << dimension << ", \"" << version << "\", " << materialId << ")";
                            first = false;
                        }

                        insert << ";";
                        if (!first) {
                            lock_guard<mutex> lock(dbMutex);
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
            }
        }
    });

    for (auto& f : futures) {
        f.get();
    }

    sqlite3_close(db);
}
