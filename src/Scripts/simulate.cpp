#include <cassert>
#include <fstream>
#include <iostream>
#include <random>
#include <unordered_map>

#define MAX_NUM_ROWS 20
// #define NUM_ITERS 10
#define BLOCK_SIZE 512 // cache line size in bits.
#define NUM_BLOCKS 1000
#define NUM_DOCS BLOCK_SIZE * NUM_BLOCKS

// DESIGN NOTE: tried using go, but the publicly available binomial rng is approximately 10x slower.
// TODO: change conventions to match BitFunnel coding conventions.

std::vector<int> get_dividers(std::string filename) {
    std::ifstream data(filename);
    int input;
    std::vector<int> bin_dividers;
    while (data >> input) {
        bin_dividers.push_back(input);
    }
    assert(bin_dividers.size() == 10);
    return bin_dividers;
}


int16_t funny_draw(std::mt19937& gen,
                   std::vector<std::binomial_distribution<int16_t>>& funny_dist,
                   std::uniform_int_distribution<>& uniform,
                   std::vector<int>& bin_dividers) {
    auto pp = uniform(gen);
    int bin = -1;

    for (int i = 0; i < bin_dividers.size(); ++i) {
        if (pp <= bin_dividers[i]) {
            bin = i;
            break;
        }
    }

    // std::cout << pp << ":" << bin << std::endl;
    if (bin == -1) {
        return 512;
    }

    return funny_dist[bin](gen);
}


std::unordered_map<int, int> run_once(std::mt19937& gen, std::binomial_distribution<int16_t>& base_dist, int num_rows,
                                      std::vector<std::binomial_distribution<int16_t>>& funny_dist, std::uniform_int_distribution<>& uniform,
                                      std::string filename) {
    int num_accesses = 0;
    // TODO: check to see if doing these allocations inside run_once matters for performance.
    // We could hoist this out and just clear the vectors in here.
    std::unordered_map<int, int> block_depth;
    int16_t block;
    int local_depth;

    std::vector<int> bin_dividers;
    if (filename != "uniform-20") {
        bin_dividers = get_dividers(filename);
    }

    for (int i = 0; i < NUM_BLOCKS; ++i) {
        local_depth = 1;
        if (filename == "uniform-20") {
            block = base_dist(gen);
        } else {
            block = funny_draw(gen, funny_dist, uniform, bin_dividers);
        }
        ++num_accesses;
        // Row 0 always gets accessed.
        // TODO: add extra access for soft-deleted row?
        for (int j = 1; j < num_rows && block != 0; ++j) {
            ++local_depth;
            int16_t row_num_set;
            if (filename == "uniform-20") {
                row_num_set = base_dist(gen);
            } else {
                row_num_set = funny_draw(gen, funny_dist, uniform, bin_dividers);
            }
            float previous_fraction = static_cast<float>(block) / 512.0;
            std::binomial_distribution<int16_t> intersection_dist(row_num_set,previous_fraction);
            block = intersection_dist(gen);
        }
        ++block_depth[local_depth];
    }
    return block_depth;
}

int main()
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::binomial_distribution<int16_t> base_dist(512, 0.2);

    // Quick hack to generate a set of distributions that match the
    // distributions for a known bug.
    std::vector<std::binomial_distribution<int16_t>> funny_dist;
    funny_dist.push_back(std::binomial_distribution<int16_t>(512, 0.05));
    funny_dist.push_back(std::binomial_distribution<int16_t>(512, 0.15));
    funny_dist.push_back(std::binomial_distribution<int16_t>(512, 0.25));
    funny_dist.push_back(std::binomial_distribution<int16_t>(512, 0.35));
    funny_dist.push_back(std::binomial_distribution<int16_t>(512, 0.45));
    funny_dist.push_back(std::binomial_distribution<int16_t>(512, 0.55));
    funny_dist.push_back(std::binomial_distribution<int16_t>(512, 0.65));
    funny_dist.push_back(std::binomial_distribution<int16_t>(512, 0.75));
    funny_dist.push_back(std::binomial_distribution<int16_t>(512, 0.85));
    funny_dist.push_back(std::binomial_distribution<int16_t>(512, 0.95));
    funny_dist.push_back(std::binomial_distribution<int16_t>(512, 0.9999999));

    std::uniform_int_distribution<> uniform(1, 10000);

    // for (int i = 1; i <= MAX_NUM_ROWS; ++i) {
    //     std::cout << i;
    //     if (i <= MAX_NUM_ROWS-1) {
    //         std::cout << ",";
    //     }
    // }
    // std::cout << std::endl;

    auto block_depth = run_once(gen, base_dist, MAX_NUM_ROWS,
                                funny_dist, uniform,
                                "uniform-20");
    for (const auto & dd : block_depth) {
        std::cout << dd.first << "," << dd.second << ",uniform20" << std::endl;
    }
    block_depth = run_once(gen, base_dist, MAX_NUM_ROWS,
                           funny_dist, uniform,
                           "bf-old");
    for (const auto & dd : block_depth) {
        std::cout << dd.first << "," << dd.second << ",actual" << std::endl;
    }
    return 0;
}
