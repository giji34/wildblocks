#include <minecraft-file.hpp>
#include <iostream>
#include <sstream>
#include <fstream>
#include <set>
#include "hwm/task/task_queue.hpp"
#include <unistd.h>
#include <filesystem>

using namespace std;
using namespace mcfile;

static void print_description() {
    cerr << "wildblocks" << endl;
    cerr << "SYNOPSYS" << endl;
    cerr << "    wildblocks -f db_directory_path -w world_directory -d dimension -v minecraft_version" << endl;
    cerr << "DIMENSION" << endl;
    cerr << "    0:  Overworld" << endl;
    cerr << "    -1: The Nether" << endl;
    cerr << "    1:  The End" << endl;
    cerr << "MINECRAFT VERSION" << endl;
    cerr << "    1.13 etc." << endl;
}

// minecraft:grass_block[snowy=false]
static string GetBlockData(shared_ptr<Block> const& block) {
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
    string dbDir;
    string worldDir;
    int dimension = 0;
    string version;

    int opt;
    opterr = 0;
    while ((opt = getopt(argc, argv, "f:w:d:v:c")) != -1) {
        switch (opt) {
            case 'f':
                dbDir = optarg;
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
    cout << "db:        " << dbDir << endl;
    cout << "world:     " << worldDir << endl;
    cout << "dimension: " << dimension << endl;
    cout << "version:   " << version << endl;

    namespace fs = std::filesystem;
    fs::path const rootDir = fs::path(dbDir) / version / to_string(dimension);
    
    if (!fs::exists(rootDir)) {
        fs::create_directories(rootDir);
    }
    if (!fs::is_directory(rootDir)) {
        cerr << "\"" << rootDir << "\" がディレクトリじゃない" << endl;
        exit(1);
    }

    map<string, int> palette;
    mutex paletteMutex;
    fs::path paletteFile = rootDir / "palette.txt"s;
    {
        ifstream paletteStream(paletteFile.c_str());
        string line;
        int index = 0;
        while (getline(paletteStream, line)) {
            if (palette.find(line) != palette.end()) {
                cerr << "palette 内の BlockData が重複しています: " << index << " 行目, line=\"" << line << "\"" << endl;
                exit(1);
            }
            palette.insert(make_pair(line, index));
            index++;
        }
    }

    hwm::task_queue q(thread::hardware_concurrency());
    vector<future<void>> futures;
    string const kAirBlockName = blocks::Name(blocks::minecraft::air);
    mutex logMutex;

    World world(worldDir);
    world.eachRegions([=, &futures, &q, &palette, &paletteMutex, &logMutex](shared_ptr<Region> const& region) {
        futures.emplace_back(q.enqueue([=, &palette, &paletteMutex, &logMutex](shared_ptr<Region> const& region) {
            bool e = false;
            region->loadAllChunks(e, [=, &palette, &paletteMutex, &logMutex](Chunk const& chunk) {
                fs::path file = rootDir / ("c." + to_string(chunk.fChunkX) + "." + to_string(chunk.fChunkZ) + ".idx");
                if (fs::exists(file)) {
                    return true;
                }
                vector<int> materialIds(16 * 16 * 256, -1);
                vector<string> blockDataList(16 * 16 * 256, kAirBlockName);
                for (int y = 0; y < 256; y++) {
                    for (int z = chunk.minBlockZ(); z <= chunk.maxBlockZ(); z++) {
                        for (int x = chunk.minBlockX(); x <= chunk.maxBlockX(); x++) {
                            shared_ptr<Block> block = chunk.blockAt(x, y, z);
                            if (!block) {
                                continue;
                            }
                            int index = ChunkLocalIndexFromBlockXYZ(x, y, z, chunk.minBlockX(), chunk.minBlockZ());
                            string blockData = GetBlockData(block);
                            blockDataList[index] = blockData;
                        }
                    }
                }
                map<tuple<int, int, int>, string> unknownMaterials;
                {
                    set<string> unknownMaterialNames;
                    lock_guard<mutex> lk(paletteMutex);
                    for (int y = 0; y < 256; y++) {
                        for (int z = chunk.minBlockZ(); z <= chunk.maxBlockZ(); z++) {
                            for (int x = chunk.minBlockX(); x <= chunk.maxBlockX(); x++) {
                                int index = ChunkLocalIndexFromBlockXYZ(x, y, z, chunk.minBlockX(), chunk.minBlockZ());
                                string blockData = blockDataList[index];
                                if (palette.find(blockData) == palette.end()) {
                                    unknownMaterials.insert(make_pair(make_tuple(x, y, z), blockData));
                                    unknownMaterialNames.insert(blockData);
                                } else {
                                    int materialId = palette[blockData];
                                    materialIds[index] = materialId;
                                }
                            }
                        }
                    }
                    vector<string>().swap(blockDataList);

                    if (!unknownMaterials.empty()) {
                        ofstream f(paletteFile.c_str(), ios::app);
                        int idx = palette.size();
                        for (auto it = unknownMaterialNames.begin(); it != unknownMaterialNames.end(); it++) {
                            string blockData = *it;
                            f << blockData << endl;
                            palette.insert(make_pair(blockData, idx));
                            idx++;
                        }
                    }
                }

                for (auto it = unknownMaterials.begin(); it != unknownMaterials.end(); it++) {
                    auto xyz = it->first;
                    string blockData = it->second;
                    auto found = palette.find(blockData);
                    if (found == palette.end()) {
                        cerr << "データ不整合" << endl;
                        exit(1);
                    }
                    int index = ChunkLocalIndexFromBlockXYZ(get<0>(xyz), get<1>(xyz), get<2>(xyz), chunk.minBlockX(), chunk.minBlockZ());
                    materialIds[index] = found->second;
                }

                if (!any_of(materialIds.begin(), materialIds.end(), [](int v) { return v > 0; })) {
                    return true;
                }
                vector<uint8_t> blob;
                int airBlockMaterialId = palette[kAirBlockName];
                for (int i = 0; i < materialIds.size(); i++) {
                    int materialId = materialIds[i];
                    if (materialId < 0) {
                        materialId = airBlockMaterialId;
                    }
                    AppendInt(blob, (uint32_t)materialId);
                }
                mcfile::detail::Compression::compress(blob);
                
                {
                    lock_guard<mutex> lk(logMutex);
                    cout << file.filename().string() << endl;
                }

                FILE *fp = fopen(file.c_str(), "wb");
                fwrite(blob.data(), blob.size(), 1, fp);
                fclose(fp);
                return true;
            });
        }, region));
    });
    
    for (auto& f : futures) {
        f.get();
    }
}
