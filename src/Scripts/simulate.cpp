#include <cassert>
#include <fstream>
#include <iostream>
#include <random>
#include <unordered_map>

#define MAX_NUM_ROWS 20
// #define NUM_ITERS 10
#define BLOCK_SIZE 512 // cache line size in bits.
#define NUM_BLOCKS 1000000
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

int16_t draw_block(std::mt19937& gen,
                   std::vector<std::binomial_distribution<int16_t>>& block_dist,
                   std::vector<std::binomial_distribution<int16_t>>& funny_dist,
                   std::uniform_int_distribution<>& uniform,
                   std::vector<int> bin_dividers,
                   std::string filename) {
        if (filename == "uniform-20") {
            return block_dist[2](gen);
        } else if (filename == "split-10-40") {
            if (uniform(gen) <= 8000) {
                return block_dist[1](gen);
            } else {
                return block_dist[4](gen);
            }
        } else {
            return funny_draw(gen, funny_dist, uniform, bin_dividers);
        }
        throw;
        return 0;
}

std::unordered_map<int, int> run_once(std::mt19937& gen, std::vector<std::binomial_distribution<int16_t>>& block_dist, int num_rows,
                                      std::vector<std::binomial_distribution<int16_t>>& funny_dist, std::uniform_int_distribution<>& uniform,
                                      std::string filename) {
    int num_accesses = 0;
    // TODO: check to see if doing these allocations inside run_once matters for performance.
    // We could hoist this out and just clear the vectors in here.
    std::unordered_map<int, int> block_depth;
    int16_t block;
    int local_depth;

    std::vector<int> bin_dividers;
    if (filename != "uniform-20" && filename != "split-10-40") {
        bin_dividers = get_dividers(filename);
    }

    for (int i = 0; i < NUM_BLOCKS; ++i) {
        local_depth = 1;
        block = draw_block(gen, block_dist, funny_dist, uniform, bin_dividers, filename);
        ++num_accesses;
        // Row 0 always gets accessed.
        // TODO: add extra access for soft-deleted row?
        for (int j = 1; j < num_rows && block != 0; ++j) {
            ++local_depth;
            int16_t row_num_set = draw_block(gen, block_dist, funny_dist, uniform, bin_dividers, filename);
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
    std::vector<std::binomial_distribution<int16_t>> block_dist;
    for (float i = 0.0; i < 1.0; i += 0.1) {
        std::binomial_distribution<int16_t> block_draw(512, i);
        block_dist.push_back(block_draw);
    }

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
    std::vector<std::string> names {"uniform-20", "split-10-40", "bf-old", "bf-new"};

    for (auto const & name : names) {
        auto block_depth = run_once(gen, block_dist, MAX_NUM_ROWS,
                                    funny_dist, uniform,
                                    name);
        for (const auto & dd : block_depth) {
            std::cout << dd.first << "," << dd.second << "," << name << std::endl;
       }
    }
    return 0;
}
