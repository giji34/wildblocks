#include <minecraft-file.hpp>
#include <iostream>
#include <sqlite3.h>
#include <sstream>
#include <set>
#include "hwm/task/task_queue.hpp"
#include <unistd.h>

using namespace std;
using namespace mcfile;

static void print_description() {
    cerr << "wildblocks" << endl;
    cerr << "SYNOPSYS" << endl;
    cerr << "    wildblocks -f db_file_path -w world_directory -d dimension -v minecraft_version" << endl;
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

static int queryVersionCallback(void *user, int argc, char **argv, char **columnName) {
    int *v = (int *)user;
    for (int i = 0; i < argc; i++) {
        if (string(columnName[i]) == "id") {
            int t;
            if (sscanf(argv[i], "%d", &t) == 1) {
                *v = t;
                break;
            }
        }
    }
    return 0;
}

static int queryMaterialUsedCallback(void *user, int argc, char **argv, char **columnName) {
    map<int, int> *current = (map<int, int> *)user;
    int materialId = -1;
    int used = 0;
    for (int i = 0; i < argc; i++) {
        string column(columnName[i]);
        int v;
        if (sscanf(argv[i], "%d", &v) != 1) {
            continue;
        }
        if (column == "id") {
            materialId = v;
        } else if (column == "used") {
            used = v;
        }
    }
    if (materialId > 0) {
        current->insert(make_pair(materialId, used));
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

static int ChunkLocalIndexFromBlockXYZ(int x, int y, int z, int minBlockX, int minBlockZ) {
    int offsetX = x - minBlockX;
    int offsetZ = z - minBlockZ;
    return y * 16 * 16 + offsetZ * 16 + offsetX;
}

static void AppendInt(vector<uint8_t> &buffer, uint32_t data) {
    while (data > 0) {
        uint8_t v = (uint8_t)(0x7F & data);
        data = data >> 7;
        if (data > 0) {
            v = v | 0x80;
        }
        buffer.push_back(v);
    }
}

int main(int argc, char *argv[]) {
    string dbFile;
    string worldDir;
    int dimension = 0;
    string version;

    int opt;
    opterr = 0;
    while ((opt = getopt(argc, argv, "f:w:d:v:c")) != -1) {
        //コマンドライン引数のオプションがなくなるまで繰り返す
        switch (opt) {
            case 'f':
                dbFile = optarg;
                break;
            case 'w':
                worldDir = optarg;
                break;
            case 'd':
                if (sscanf(optarg, "%d", &dimension) != 1) {
                    print_description();
                    return -1;
                }
                break;
            case 'v':
                version = optarg;
                break;
            default:
                print_description();
                return -1;
        }
    }
    
    if (argc < 5) {
        print_description();
        return -1;
    }
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
            data string unique not null,
            used integer not null default 0
        );)";
    if (sqlite3_exec(db, createMaterialsTable.c_str(), nullptr, nullptr, &error) != SQLITE_OK) {
        cerr << error << endl;
        sqlite3_free(error);
        sqlite3_close(db);
        return -1;
    }

    string createWildBlocksTable = R"(
        create table if not exists wild_chunks (
            x integer not null,
            z integer not null,
            data blob not null,
            dimension integer not null,
            version_id integer not null
        );)";
    if (sqlite3_exec(db, createWildBlocksTable.c_str(), nullptr, nullptr, &error) != SQLITE_OK) {
        cerr << error << endl;
        sqlite3_free(error);
        sqlite3_close(db);
        return -1;
    }

    string createVersionsTable = R"(create table if not exists versions (
        id integer primary key autoincrement,
        version text not null
    );)";
    if (sqlite3_exec(db, createVersionsTable.c_str(), nullptr, nullptr, &error) != SQLITE_OK) {
        cerr << error << endl;
        sqlite3_free(error);
        sqlite3_close(db);
        return -1;
    }

    int versionId = -1;
    if (sqlite3_exec(db, (string("select * from versions where version = ") + version).c_str(), queryVersionCallback, &versionId, &error) != SQLITE_OK) {
        cerr << error << endl;
        sqlite3_free(error);
        sqlite3_close(db);
        return -1;
    }
    if (versionId < 0) {
        if (sqlite3_exec(db, (string("insert into versions (version) values (") + version + string(");")).c_str(), nullptr, nullptr, &error) != SQLITE_OK) {
            cerr << error << endl;
            sqlite3_free(error);
            sqlite3_close(db);
            return -1;
        }
        if (sqlite3_exec(db, (string("select * from versions where version = ") + version).c_str(), queryVersionCallback, &versionId, &error) != SQLITE_OK) {
            cerr << error << endl;
            sqlite3_free(error);
            sqlite3_close(db);
            return -1;
        }
    }
    if (versionId < 0) {
        cerr << "versionId が取得できない" << endl;
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
    
    string createIndex = "create unique index if not exists wild_chunks_unique_coordinate on wild_chunks (x, z, dimension, version_id);";
    if (sqlite3_exec(db, createIndex.c_str(), nullptr, nullptr, &error) != SQLITE_OK) {
        cerr << error << endl;
        sqlite3_free(error);
        sqlite3_close(db);
        return -1;
    }

    set<pair<int, int>> existingChunks;
    if (sqlite3_exec(db, "select count(*), x as chunkX, z as chunkZ from wild_chunks group by chunkX, chunkZ;", queryExistingChunksCallback, &existingChunks, &error) != SQLITE_OK) {
        cerr << error << endl;
        sqlite3_free(error);
        sqlite3_close(db);
        return -1;
    }
    
    hwm::task_queue q(thread::hardware_concurrency());
    vector<future<void>> futures;
    mutex dbMutex;
    string const kAirBlockName = blocks::Name(blocks::minecraft::air);

    World world(worldDir);
    world.eachRegions([=, &existingChunks, &futures, &q, &blockLut, &dbMutex, &lutMutex](shared_ptr<Region> const& region) {
        for (int localChunkX = 0; localChunkX <= 32; localChunkX++) {
            for (int localChunkZ = 0; localChunkZ <= 32; localChunkZ++) {
                int chunkX = region->fX * 32 + localChunkX;
                int chunkZ = region->fZ * 32 + localChunkZ;
                if (existingChunks.find(make_pair(chunkX, chunkZ)) != existingChunks.end()) {
                    continue;
                }
                futures.emplace_back(q.enqueue([=, &dbMutex, &lutMutex, &blockLut]() {
                    bool error = false;
                    region->loadChunk(localChunkX, localChunkZ, error, [=, &dbMutex, &lutMutex, &blockLut](Chunk const& chunk) {
                        vector<int> materialIds(16 * 16 * 256, -1);
                        map<int, int> materialUsage;
                        {
                            map<tuple<int, int, int>, string> unknownMaterials;
                            set<string> unknownMaterialNames;

                            lock_guard<mutex> lk(lutMutex);
                            for (int y = 0; y < 256; y++) {
                                for (int z = chunk.minBlockZ(); z <= chunk.maxBlockZ(); z++) {
                                    for (int x = chunk.minBlockX(); x <= chunk.maxBlockX(); x++) {
                                        shared_ptr<Block> block = chunk.blockAt(x, y, z);
                                        if (!block) {
                                            continue;
                                        }
                                        string blockData = getBlockData(block);
                                        if (blockLut.find(blockData) == blockLut.end()) {
                                            unknownMaterials.insert(make_pair(make_tuple(x, y, z), blockData));
                                            unknownMaterialNames.insert(blockData);
                                        } else {
                                            int index = ChunkLocalIndexFromBlockXYZ(x, y, z, chunk.minBlockX(), chunk.minBlockZ());
                                            int materialId = blockLut[blockData];
                                            materialIds[index] = materialId;
                                            materialUsage[materialId] += 1;
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
                                    int index = ChunkLocalIndexFromBlockXYZ(get<0>(xyz), get<1>(xyz), get<2>(xyz), chunk.minBlockX(), chunk.minBlockZ());
                                    materialIds[index] = found->second;
                                    materialUsage[found->second] += 1;
                                }
                            }
                        }

                        if (!any_of(materialIds.begin(), materialIds.end(), [](int v) { return v > 0; })) {
                            return true;
                        }
                        vector<uint8_t> blob;
                        int airBlockMaterialId = blockLut[kAirBlockName];
                        for (int i = 0; i < materialIds.size(); i++) {
                            int materialId = materialIds[i];
                            if (materialId < 0) {
                                materialId = airBlockMaterialId;
                            }
                            AppendInt(blob, (uint32_t)materialId);
                        }
                        
                        {
                            lock_guard<mutex> lk(dbMutex);
                            sqlite3_stmt *st;
                            string sql("insert into wild_chunks (x, z, data, dimension, version_id) values (?, ?, ?, ?, ?);");
                            if (sqlite3_prepare_v2(db, sql.c_str(), sql.size(), &st, nullptr) != SQLITE_OK) {
                                cerr << "sqlite3_prepare_v2 failed" << endl;
                                exit(1);
                            }
                            sqlite3_bind_int(st, 1, chunkX);
                            sqlite3_bind_int(st, 2, chunkZ);
                            sqlite3_bind_blob(st, 3, blob.data(), blob.size(), nullptr);
                            sqlite3_bind_int(st, 4, dimension);
                            sqlite3_bind_int(st, 5, versionId);
                            if (sqlite3_step(st) != SQLITE_DONE) {
                                cerr << "sqlite3_step failed" << endl;
                                exit(1);
                            }
                            sqlite3_finalize(st);
                            
                            map<int, int> materialIdCurrentUsage;
                            char *e = nullptr;
                            if (sqlite3_exec(db, "select id, used from materials", queryMaterialUsedCallback, &materialIdCurrentUsage, &e) != SQLITE_OK) {
                                cerr << e << endl;
                                sqlite3_free(e);
                                exit(1);
                            }
                            map<int, int> updatedMaterialIdUsage;
                            for (auto it = materialUsage.begin(); it != materialUsage.end(); it++) {
                                int materialId = it->first;
                                int used = it->second + materialIdCurrentUsage[materialId];
                                ostringstream ss;
                                ss << "update materials set used = " << used << " where id = " << materialId << " limit 1";
                                if (sqlite3_exec(db, ss.str().c_str(), nullptr, nullptr, &e) != SQLITE_OK) {
                                    cerr << e << endl;
                                    sqlite3_free(e);
                                    exit(1);
                                }
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
