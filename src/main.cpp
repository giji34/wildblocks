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
namespace fs = std::filesystem;

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

static void AppendInt(vector<uint8_t> &buffer, uint32_t data) {
    while (true) {
        uint8_t v = (uint8_t)(0x7F & data);
        data = data >> 7;
        if (data > 0) {
            v = v | (uint8_t)0x80;
        }
        buffer.push_back(v);
        if (data == 0) {
            break;
        }
    }
}

static void CreateWorldBlockPalette(World const& world, vector<string> &result, int numRegions, fs::path rootDir) {
    hwm::task_queue q(thread::hardware_concurrency());
    vector<future<map<string, int>>> futures;
    mutex countMutex;
    int finishedRegions = 0;

    world.eachRegions([=, &q, &futures, &finishedRegions, &countMutex](shared_ptr<Region> const& region) {
        futures.emplace_back(q.enqueue([=, &finishedRegions, &countMutex](shared_ptr<Region> const& region) {
            map<string, int> regionPartial;
            for (int lcx = 0; lcx < 32; lcx++) {
                int const chunkX = region->fX * 32 + lcx;
                for (int lcz = 0; lcz < 32; lcz++) {
                    int const chunkZ = region->fZ * 32 + lcz;
                    fs::path file = rootDir / ("c." + to_string(chunkX) + "." + to_string(chunkZ) + ".idx");
                    if (fs::exists(file)) {
                        continue;
                    }
                    bool e = false;
                    region->loadChunk(lcx, lcz, e, [&regionPartial](Chunk const& chunk) {
                        for (int y = 0; y < 256; y++) {
                            for (int z = chunk.minBlockZ(); z <= chunk.maxBlockZ(); z++) {
                                for (int x = chunk.minBlockX(); x <= chunk.maxBlockX(); x++) {
                                    shared_ptr<Block> block = chunk.blockAt(x, y, z);
                                    if (!block) {
                                        continue;
                                    }
                                    string blockData = GetBlockData(block);
                                    regionPartial[blockData] += 1;
                                }
                            }
                        }
                        return true;
                    });
                }
            }
            {
                lock_guard<mutex> lk(countMutex);
                finishedRegions++;
                cout << "palette: " << finishedRegions << "/" << numRegions << "\t" << (float(finishedRegions) / float(numRegions) * 100.0f) << "%" << endl;
            }
            return regionPartial;
        }, region));
    });
    map<string, int> blockAndUsedCount;
    for (auto &f : futures) {
        map<string, int> partial = f.get();
        for_each(partial.begin(), partial.end(), [&blockAndUsedCount](auto const& it) {
            blockAndUsedCount[it.first] += it.second;
        });
    }

    vector<pair<string, int>> buffer;
    buffer.reserve(blockAndUsedCount.size());
    for (auto it = blockAndUsedCount.begin(); it != blockAndUsedCount.end(); it++) {
        buffer.push_back(make_pair(it->first, it->second));
    }
    sort(buffer.begin(), buffer.end(), [](auto const& a, auto const& b) {
        return a.second > b.second;
    });
    result.clear();
    result.reserve(buffer.size());
    for_each(buffer.begin(), buffer.end(), [&result](auto const& it) {
        result.push_back(it.first);
    });

    cout << "palette created: " << blockAndUsedCount.size() << " entries" << endl;
}

static int CountRegionFiles(string worldDir) {
    int numRegions = 0;
    for (auto const& e : fs::directory_iterator(fs::path(worldDir) / "region")) {
        int x, z;
        if (sscanf(e.path().filename().c_str(), "r.%d.%d.mca", &x, &z) == 2) {
            numRegions++;
        }
    }
    return numRegions;
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

    fs::path const rootDir = fs::path(dbDir) / version / to_string(dimension);
    
    if (!fs::exists(rootDir)) {
        fs::create_directories(rootDir);
    }
    if (!fs::is_directory(rootDir)) {
        cerr << "\"" << rootDir << "\" がディレクトリじゃない" << endl;
        exit(1);
    }

    World world(worldDir);
    int numRegions = CountRegionFiles(worldDir);

    fs::path paletteFile = rootDir / "palette.txt"s;
    map<string, int> palette;
    {
        ifstream paletteStream(paletteFile.c_str());
        string line;
        int index = 0;
        while (getline(paletteStream, line)) {
            palette.insert(make_pair(line, index));
            index++;
        }
    }
    
    vector<string> paletteBlockData;
    CreateWorldBlockPalette(world, paletteBlockData, numRegions, rootDir);
    
    {
        vector<string> newBlockData;
        for (auto it = paletteBlockData.begin(); it != paletteBlockData.end(); it++) {
            string blockData = *it;
            auto found = palette.find(blockData);
            if (found != palette.end()) {
                continue;
            }
            newBlockData.push_back(blockData);
            palette.insert(make_pair(blockData, palette.size()));
        }
        
        ofstream paletteStream(paletteFile.c_str(), ios::app);
        for (auto it = newBlockData.begin(); it != newBlockData.end(); it++) {
            string blockData = *it;
            paletteStream << blockData << endl;
        }
    }

    hwm::task_queue q(thread::hardware_concurrency());
    vector<future<void>> futures;
    string const kAirBlockName = blocks::Name(blocks::minecraft::air);
    mutex logMutex;
    int finishedRegions = 0;
    
    world.eachRegions([=, &futures, &q, &logMutex, &finishedRegions](shared_ptr<Region> const& region) {
        futures.emplace_back(q.enqueue([=, &logMutex, &finishedRegions](shared_ptr<Region> const& region) {
            for (int lcx = 0; lcx < 32; lcx++) {
                int const chunkX = region->fX * 32 + lcx;
                for (int lcz = 0; lcz < 32; lcz++) {
                    int const chunkZ = region->fZ * 32 + lcz;
                    fs::path file = rootDir / ("c." + to_string(chunkX) + "." + to_string(chunkZ) + ".idx");
                    if (chunkX == 8 && chunkZ == 20) {
                        int a = 0;
                        cerr << a << endl;
                    }
                    if (fs::exists(file)) {
                        continue;
                    }
                    bool e = false;
                    region->loadChunk(lcx, lcz, e, [=](Chunk const& chunk) {
                        fs::path file = rootDir / ("c." + to_string(chunk.fChunkX) + "." + to_string(chunk.fChunkZ) + ".idx");
                        vector<uint8_t> blob;
                        for (int y = 0; y < 256; y++) {
                            for (int z = chunk.minBlockZ(); z <= chunk.maxBlockZ(); z++) {
                                for (int x = chunk.minBlockX(); x <= chunk.maxBlockX(); x++) {
                                    shared_ptr<Block> block = chunk.blockAt(x, y, z);
                                    string blockData;
                                    if (block) {
                                        blockData = GetBlockData(block);
                                    } else {
                                        blockData = kAirBlockName;
                                    }
                                    auto found = palette.find(blockData);
                                    if (found == palette.end()) {
                                        cerr << "データ不整合" << endl;
                                        exit(1);
                                    }
                                    AppendInt(blob, found->second);
                                }
                            }
                        }
                        mcfile::detail::Compression::compress(blob);

                        FILE *fp = fopen(file.c_str(), "w+b");
                        fwrite(blob.data(), blob.size(), 1, fp);
                        fclose(fp);
                        cout << file.string() << endl;
                        return true;
                    });
                    if (e) {
                        cerr << "caught error for chunk: " << file.string() << endl;
                    }
                }
            }

            lock_guard<mutex> lk(logMutex);
            finishedRegions++;
            cout << finishedRegions << "/" << numRegions << "\t" << float(finishedRegions * 100.0f / numRegions) << "%" << endl;
        }, region));
    });
    
    for (auto& f : futures) {
        f.get();
    }
}
