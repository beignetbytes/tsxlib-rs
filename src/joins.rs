use std::cmp;
use std::hash::{Hash};
use std::collections::{HashMap};

use serde::{Serialize};
use std::convert::TryInto;
use crate::index::{HashableIndex};


pub struct JoinEngine<'a, TIndex: Serialize + Hash + Clone + cmp::Eq + cmp::Ord> {
    pub idx_this : &'a HashableIndex<TIndex>,
    pub idx_other : &'a HashableIndex<TIndex>
}

pub struct IndexJoinPair{
    pub this_idx: usize,
    pub other_idx: usize 
}

pub struct IndexJoinPotentiallyUnmatchedPair{
    pub this_idx: usize,
    pub other_idx: Option<usize> 
}

pub fn prior_func(idx: usize) -> usize{
    if idx == 0 { 
        0 
    } else{
        idx-1
    }
}
pub fn fwd_func(idx: usize, otherlen: usize) -> usize{
    if idx >= otherlen-1 { 
        otherlen-1 
    } else {
        idx+1
    }
}


impl <'a, TIndex: Serialize + Hash + Clone + cmp::Eq + cmp::Ord> JoinEngine<'a, TIndex>{

    #[cfg(feature = "hash_precompare")]
    fn hash_index(&self, index: &HashableIndex<TIndex>) -> u64{ 
        let bytes = bincode::serialize(&index.values).unwrap();
        seahash::hash(&bytes)
    }

    #[cfg(feature = "hash_precompare")]
    fn hash_precompare(&self) -> bool{
        if self.idx_this.len() == self.idx_other.len(){
            self.hash_index(self.idx_this) == self.hash_index(self.idx_other)
        } else{
            false
        }
    }
    fn index_is_same(&self) -> bool{ #![allow(unused_assignments)] #![allow(unused_mut)]
        let mut out = false; 
        #[cfg(feature = "hash_precompare")]{
            out = self.hash_precompare();
        }               
        out
    }

    fn gen_base_lookup(&self,hashbase: &HashableIndex<TIndex>) -> HashMap<TIndex, usize>
    {
        let mut lookup: HashMap<TIndex, usize> = HashMap::with_capacity(hashbase.len());    //reserve to avoid reallocate
        hashbase.iter().enumerate().for_each(|(idx, key)| {
            lookup.insert(key.clone(), idx);
        });
        lookup
    }
    
    /// Hash inner join
    pub fn get_inner_hash_joined_indicies(&self) -> Vec<IndexJoinPair>
    {
        if self.index_is_same() {
            //if we are the same just skip this whole thing
            self.idx_this.iter().enumerate().map(|(idx,_x)| IndexJoinPair{this_idx : idx,other_idx : idx}).collect()
        }
        else {
            let (lookup, this_shorter) = match self.idx_this.len() <= self.idx_this.len() { 
                true => (self.gen_base_lookup(&self.idx_this),false),
                false => (self.gen_base_lookup(&self.idx_other),true)
            };
    
            if this_shorter {
                let res: Vec<IndexJoinPair> = self.idx_this.iter().enumerate().map(|(idx_this, key)| {
                    if let Some(idx_other) = lookup.get(&key) {                    
                        Some(IndexJoinPair { 
                            this_idx : idx_this, 
                            other_idx : *idx_other
                        })
                    }
                    else{
                        None
                    }
                })
                .filter(|x| x.is_some())
                .map(|x| x.unwrap())
                .collect();
    
                res
            } else {
                let res = self.idx_other.iter().enumerate().map(|(idx_other, key)| {
                    if let Some(idx_this) = lookup.get(&key) {                    
                        Some(IndexJoinPair { 
                            this_idx : *idx_this, 
                            other_idx : idx_other
                        })
                    }
                    else{
                        None
                    }
                })
                .filter(|x| x.is_some())
                .map(|x| x.unwrap())
                .collect();
    
                res
            }
        }

    }
    
    /// Hash join left. 
    /// All left values are joined so no Option<usize> on the base index
    pub fn get_left_hash_joined_indicies(&self) -> Vec<IndexJoinPotentiallyUnmatchedPair>
    {
        if self.index_is_same() {
            //if we are the same just skip this whole thing
            self.idx_this.iter().enumerate().map(|(idx,_x)| IndexJoinPotentiallyUnmatchedPair{this_idx : idx,other_idx : Some(idx)}).collect()
        }
        else{
            let lookup = self.gen_base_lookup(&self.idx_other);
    
            let res: Vec<IndexJoinPotentiallyUnmatchedPair> = self.idx_this.iter().enumerate().map(|(idx_this, key)| {
                if let Some(idx_other) = lookup.get(&key) {                    
                    IndexJoinPotentiallyUnmatchedPair { 
                        this_idx : idx_this, 
                        other_idx : Some(*idx_other)
                    }
                }
                else{
                    IndexJoinPotentiallyUnmatchedPair { 
                        this_idx : idx_this, 
                        other_idx : None
                    }
                }
            })
            .collect();
            res
        }
    }

    pub fn get_left_merge_joined_indicies(&self) -> Vec<IndexJoinPotentiallyUnmatchedPair>
    {
        if self.index_is_same() {
            //if we are the same just skip this whole thing
            self.idx_this.iter().enumerate().map(|(idx,_x)| IndexJoinPotentiallyUnmatchedPair{this_idx : idx,other_idx : Some(idx)}).collect()
        }
        else{
            let mut output: Vec<IndexJoinPotentiallyUnmatchedPair> = Vec::new();
            let mut pos1: usize = 0;
            let mut pos2: usize = 0;

            while pos1 < self.idx_this.len() && pos2 < self.idx_other.len() {
                match self.idx_this[pos1].cmp(&self.idx_other[pos2]) {
                    cmp::Ordering::Greater => {
                        output.push(IndexJoinPotentiallyUnmatchedPair{
                            this_idx: pos1,
                            other_idx: None
                        });
                        pos2 += 1;
                    },
                    cmp::Ordering::Less => {
                        output.push(IndexJoinPotentiallyUnmatchedPair{
                            this_idx: pos1,
                            other_idx: None
                        });
                        pos1 += 1;
                    },
                    cmp::Ordering::Equal => {
                        output.push(IndexJoinPotentiallyUnmatchedPair{
                            this_idx: pos1,
                            other_idx: Some(pos2)
                        });
                        pos1 += 1;
                        pos2 += 1;
                    }
                }
            }
            output
        }
    }

    /// merge sort joirn join a and b.
    pub fn get_inner_merge_joined_indicies(&self) -> Vec<IndexJoinPair>
    {
        if self.index_is_same() {
            //if we are the same just skip this whole thing
            self.idx_this.iter().enumerate().map(|(idx,_x)| IndexJoinPair{this_idx : idx,other_idx : idx}).collect()
        }
        else {
            let mut output: Vec<IndexJoinPair> = Vec::new();
            let mut pos1: usize = 0;
            let mut pos2: usize = 0;

            while pos1 < self.idx_this.len() && pos2 < self.idx_other.len() {
                match self.idx_this[pos1].cmp(&self.idx_other[pos2]) {
                    cmp::Ordering::Greater => {
                        pos2 += 1;
                    },
                    cmp::Ordering::Less => {
                        pos1 += 1;
                    },
                    cmp::Ordering::Equal => {
                        output.push(IndexJoinPair{
                            this_idx: pos1,
                            other_idx: pos2
                        });
                        pos1 += 1;
                        pos2 += 1;
                    }
                }
            }
            output
        }
    }
    
    /// merge sort joirn join a and b.
    pub fn get_asof_merge_joined_indicies(&self, compare_func: Option<Box<dyn Fn(&TIndex,&TIndex,&TIndex)->(cmp::Ordering,i64)>>,other_idx_func: Option<Box<dyn Fn(usize)->usize>>) -> Vec<IndexJoinPotentiallyUnmatchedPair>
    { #![allow(clippy::type_complexity)]
        if self.index_is_same() {
            //if we are the same just skip this whole thing
            self.idx_this.iter().enumerate().map(|(idx,_x)| IndexJoinPotentiallyUnmatchedPair{this_idx : idx,other_idx : Some(idx)}).collect()
        }
        else {
            let mut output: Vec<IndexJoinPotentiallyUnmatchedPair> = Vec::new();
            let mut pos1: usize = 0;
            let mut pos2: usize = 0;

            let comp_func = match compare_func{
                Some(func)=> func,
                None => Box::new(|this:&TIndex, other:&TIndex, _other_prior:&TIndex| (this.cmp(&other),0)) // use built in ordinal compare if no override
            };
            
            let cand_idx_func = match other_idx_func{
                Some(func) =>func,
                None => Box::new(|idx| idx)
            };

            while pos1 < self.idx_this.len() && pos2 < self.idx_other.len() {
                let comp_res = comp_func(&self.idx_this[pos1],&self.idx_other[pos2],&self.idx_other[cand_idx_func(pos2)]);
                let offset = comp_res.1;
                match comp_res.0 { 
                    // (Evaluated as,  but is actually)
                    cmp::Ordering::Greater => {
                        output.push(IndexJoinPotentiallyUnmatchedPair{
                            this_idx: pos1,
                            other_idx: None
                        });
                        pos1 += 1;
                    },
                    cmp::Ordering::Less => {
                        output.push(IndexJoinPotentiallyUnmatchedPair{
                            this_idx: pos1,
                            other_idx: None
                        });
                        pos1 += 1;
                    },
                    cmp::Ordering::Equal => {
                        let pas64:i64 = pos2.try_into().unwrap();
                        let idx0:i64 =  pas64 + offset;
                        output.push(IndexJoinPotentiallyUnmatchedPair{
                            this_idx: pos1,
                            other_idx: Some(idx0.try_into().unwrap())
                        });
                        if self.idx_this[pos1].eq(&self.idx_other[pos2]) { // only incr if things are actually equal
                            pos2 += 1;
                        }
                        pos1 += 1;
                    }
                }
            }
            output
        }
    } 
    

}


