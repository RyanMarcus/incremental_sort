// < begin copyright > 
// Copyright Ryan Marcus 2018
// 
// This file is part of incremental_radix.
// 
// incremental_radix is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// incremental_radix is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with incremental_radix.  If not, see <http://www.gnu.org/licenses/>.
// 
// < end copyright > 
extern crate rand;

use std::cmp;

enum SorterState {
    Unprepared,
    Counting,
    ComputeIndexes,
    MoveItems,
    Finished
}

pub struct IncrementalSorter {
    iterations_per_call: usize,
    to_sort: Vec<usize>,
    state: SorterState,

    // overall sort progress
    max_digit_index: u8,
    digit_index: u8,

    // set by compute_indexes, used by move_items
    new_indexes: Vec<usize>,

    // shared
    loop_index: usize,

    // storage for compute_indexes
    true_count: usize,
    false_count: usize,

    // set by bucket_counts, used by compute_indexes
    accumulator: usize
}

impl IncrementalSorter {
    pub fn new(to_sort: Vec<usize>) -> IncrementalSorter {
        let len = to_sort.len();
        return IncrementalSorter { to_sort, state: SorterState::Unprepared,
                                   new_indexes: Vec::with_capacity(len),
                                   max_digit_index: 0, digit_index: 0,
                                   loop_index: 0, accumulator: 0,
                                   true_count: 0, false_count: 0,
                                   iterations_per_call: 32};
    }

    pub fn with_iterations_per_call(to_sort: Vec<usize>, iterations_per_call: usize) -> IncrementalSorter {
        let mut to_return = IncrementalSorter::new(to_sort);
        to_return.iterations_per_call = iterations_per_call;
        return to_return;
    }

    pub fn prepare(&mut self) {
        if let SorterState::Unprepared = self.state {
            let total_bits = usize::min_value().count_zeros() + usize::min_value().count_ones();
            let fewest_leading_zeros = self.to_sort.iter().map(|itm| itm.leading_zeros()).min().unwrap();
            self.max_digit_index = (total_bits - fewest_leading_zeros) as u8;
            self.state = SorterState::Counting;
            return;
        }

        panic!("Call to IncrementalSorter prepare when not in the unprepared state");
       
    }

    fn get_bit(idx: u8, itm: usize) -> bool {
        itm & (1 << idx) != 0
    }

    // returns how many items in the "false" bucket
    fn bucket_counts(&mut self) -> bool {
        let idx = self.digit_index;
        let start = self.loop_index;
        let stop = cmp::min(start + self.iterations_per_call, self.to_sort.len());

        let count = self.to_sort[start..stop].iter()
            .filter(|&&itm| !IncrementalSorter::get_bit(idx, itm))
            .count();

        self.accumulator += count;
        self.loop_index = stop;
        return stop == self.to_sort.len();
    }

    fn compute_indexes(&mut self) -> bool {
        // first, compute the new index of each element in the vector.
        let start = self.loop_index;
        let stop = cmp::min(start + self.iterations_per_call, self.to_sort.len());
        
        for &item in self.to_sort[start..stop].iter() {
            if IncrementalSorter::get_bit(self.digit_index, item) {
                // it goes in the true bin
                self.new_indexes.push(self.true_count);
                self.true_count += 1;
            } else {
                // it goes in the false bin
                debug_assert!(self.false_count < self.accumulator);
                self.new_indexes.push(self.false_count);
                self.false_count += 1;
            }
        }

        self.loop_index = stop;

        return self.loop_index == self.to_sort.len();
    }

    fn move_items(&mut self) -> bool {
        for _ in 0..self.iterations_per_call {
            // next, move everything into the proper index.
            let idx = self.loop_index;

            if self.new_indexes[idx] == idx {
                // this element is in the correct place.
                self.loop_index += 1;
                if self.loop_index == self.to_sort.len() {
                    return true;
                }
            }

            // this element is *not* in the correct place
            let current_position = idx;
            let correct_position = self.new_indexes[idx];

            self.to_sort.swap(current_position, correct_position);
            self.new_indexes.swap(current_position, correct_position);
        }
        return false;
    }
    
    pub fn sort(&mut self) -> bool {
        match self.state {
            SorterState::ComputeIndexes => {
                if self.compute_indexes() {
                    // finished!
                    debug_assert_eq!(self.new_indexes.len(), self.to_sort.len());
                    self.state = SorterState::MoveItems;
                    self.loop_index = 0;
                }
                return false;
            },

            SorterState::MoveItems => {
                if !self.move_items() {
                    return false;
                }
                
                self.digit_index += 1;
                
                if self.digit_index == self.max_digit_index {
                    self.state = SorterState::Finished;
                    return true;
                }

                // we should return to counting for the next pass
                self.state = SorterState::Counting;
                self.new_indexes.clear();
                self.loop_index = 0;
                self.accumulator = 0;

                return false;
            },

            SorterState::Counting => {
                if self.bucket_counts() {
                    // the count is finished!
                    self.state = SorterState::ComputeIndexes;
                    self.loop_index = 0;
                    self.false_count = 0;
                    self.true_count = self.accumulator;
                }

                return false;
            }

            _ => {
                panic!("Call to sort when not in the sorting or prepared state");
            }
        };
    }

    fn get_result(self) -> Vec<usize> {
        return self.to_sort;
    }
}


#[cfg(test)]
mod tests {
    use IncrementalSorter;
    use rand::prelude::*;

    fn compare_with_stdlib_with_calls(data: &Vec<usize>, calls: usize) {
        let cpy1 = data.clone();
        let mut cpy2 = data.clone();
        
        let mut incr_sort = IncrementalSorter::new(cpy1);
        incr_sort.prepare();
        
        while !incr_sort.sort() {};

        let sorted_data = incr_sort.get_result();

        cpy2.sort();
        assert_eq!(cpy2, sorted_data);
    }
    
    fn compare_with_stdlib(data: &Vec<usize>) {
        compare_with_stdlib_with_calls(data, 1);
        compare_with_stdlib_with_calls(data, 2);
        compare_with_stdlib_with_calls(data, 64);
    }
    
    #[test]
    fn simple_example() {
        let data = vec![10, 20, 30000, 30, 5, 1, 90, 128];
        compare_with_stdlib(&data);
    }

    #[test]
    fn all_identical() {
        let data = vec![30, 30, 30, 30, 30];
        compare_with_stdlib(&data);
    }
    
    #[test]
    fn random_size_500() {
        for _ in 0..100 {
            let mut data = Vec::new();
            for _ in 0..500 {
                let v = random::<f64>();
                data.push((v * 10000.0) as usize);
            }
            compare_with_stdlib(&data);
        }
    }

    #[test]
    fn random_size_50000() {
        for _ in 0..5 {
            let mut data = Vec::new();
            for _ in 0..50000 {
                let v = random::<f64>();
                data.push((v * 10000.0) as usize);
            }
            compare_with_stdlib(&data);
        }
    }

}
