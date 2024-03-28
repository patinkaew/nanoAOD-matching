// c++ libraries include
#include <iostream>
#include <fstream>
#include <filesystem>
#include <iomanip>
#include <chrono>
#include <unordered_map>
#include <set>
#include <cstring>
#include <fmt/core.h>
#include <fmt/chrono.h>

// ROOT libraries include
#include "TFile.h"
#include "TString.h"
#include "TTree.h"
#include "TChain.h"
#include "TChainIndex.h"
#include "TTreeReader.h"
#include "TTreeReaderValue.h"
#include "TTreeReaderArray.h"
#include "TBufferFile.h"
#include "TObject.h"
#include "TBranch.h"
#include "TLeaf.h"
#include "TBranchElement.h"
#include "TLeafElement.h"
#include "TROOT.h"

// input parameters
// std::string datasetA_filelist_filename = "filelist_test1.txt";
// std::string datasetB_filelist_filename = "filelist_test2.txt";
// std::string datasetA_branchname_prefix = "Nom.";
// std::string datasetB_branchname_prefix = "PFup.";

// std::string datasetA_filelist_filename = "dataE/filelist1.txt";
// std::string datasetB_filelist_filename = "dataE/filelist2.txt";
// std::string datasetA_branchname_prefix = "22Sep2023.";
// std::string datasetB_branchname_prefix = "19Dec2023.";

std::string datasetA_filelist_filename = "dataFiles_2023D_ZB.txt"; //"ZB_2023D_filelist.txt";
std::string datasetB_filelist_filename = "hltFiles_2023D.txt"; //"AlCaJet_2023D_filelist.txt";
std::string datasetA_branchname_prefix = "ZB.";
std::string datasetB_branchname_prefix = "AlCa.";

// output parameters
//std::string out_directory = "output_test4";
//std::string out_directory = "root://hip-cms-se.csc.fi:///store/user/pinkaew/matching_output_test";
std::string out_directory = "output";
//std::string out_filename_prefix = "merge_variation_nano";
std::string out_filename_prefix = "merge_nano";
UInt_t out_file_index = 1;
Long64_t out_tree_max_size = 500000000LL; // 500 MB before compression
//Long64_t out_tree_max_size = 5000000LL;
// Long64_t out_tree_max_num_entries = 100000;

// debugging parameters
int verbose = 3;
float print_every_percent = 0.1;

// helper function defintion
TChain* build_chain(std::string filelist_filename);
void* allocate_memory_from_leaf(const char* leaf_type_name, bool singleton, Int_t length);
void append_branches_from_tree(TTree *src_tree, TTree *dst_tree, std::unordered_map<std::string, void*>& data_addresses, const std::string& prefix);
void reallocate_memory_if_any(TTree *src_tree, TTree *dst_tree, std::unordered_map<std::string, void*>& data_addresses, const std::string& prefix);
void deallocate_memory_from_leaf(const char* leaf_type_name, void* addr);
Long64_t get_tree_byte_size(TTree* tree);

// main
int main(){
    ROOT::DisableImplicitMT();
    
    if (verbose >= 2) std::cout << "Start setting up..." << std::endl;
    // stop watch
    std::chrono::steady_clock stopwatch;
    auto saved_time = stopwatch.now();
    auto current_time = stopwatch.now();
    std::chrono::duration<double> elapsed_time = current_time - saved_time;

    // setup chains
    if (verbose >= 2) std::cout << "Start building input chains..." << std::endl;
    TChain *short_chain = build_chain(datasetA_filelist_filename);
    TChain *long_chain = build_chain(datasetB_filelist_filename);
    std::string short_chain_branchname_prefix = datasetA_branchname_prefix;
    std::string long_chain_branchname_prefix = datasetB_branchname_prefix;
    Long64_t short_chain_num_entries = short_chain->GetEntries();
    Long64_t long_chain_num_entries = long_chain->GetEntries();
    Long64_t datasetA_num_entries = short_chain_num_entries;
    Long64_t datasetB_num_entries = long_chain_num_entries;
    if (verbose >= 2) std::cout << "Finish building input chains..." << std::endl;

    // swap chain if first chain is longer than the second chain
    if (short_chain_num_entries > long_chain_num_entries) {
        if (verbose >= 3) std::cout << "Swapping input chains" << std::endl;
        std::swap(short_chain, long_chain);
        std::swap(short_chain_branchname_prefix, long_chain_branchname_prefix);
        std::swap(short_chain_num_entries, long_chain_num_entries);
        datasetA_num_entries = long_chain_num_entries;
        datasetB_num_entries = short_chain_num_entries;
    }

    // set active branches
    short_chain->SetBranchStatus("*", false);
    long_chain->SetBranchStatus("*", false);

    // short_chain->SetBranchStatus("nJet", true);
    // long_chain->SetBranchStatus("nJet", true);

    // short_chain->SetBranchStatus("MET_pt", true);
    // long_chain->SetBranchStatus("MET_pt", true);

    // short_chain->SetBranchStatus("Jet_pt", true);
    // long_chain->SetBranchStatus("Jet_pt", true);

    // short_chain->SetBranchStatus("SV_chi2", true);
    // long_chain->SetBranchStatus("SV_chi2", true);

    short_chain->SetBranchStatus("*", true);
    long_chain->SetBranchStatus("*", true);

    // reserve memory so we can use to build out_tree
    // short_chain->GetEntry(0);
    // long_chain->GetEntry(0);

    // set run and event branch to active
    short_chain->SetBranchStatus("run", true); 
    short_chain->SetBranchStatus("event", true); 
    long_chain->SetBranchStatus("run", true); 
    long_chain->SetBranchStatus("event", true); 
    if (verbose >= 2) std::cout << "Finish setting up..." << std::endl;

    // build lookup indices for short chain
    if (verbose >= 1) std::cout << "Start building lookup indices with " << short_chain->GetEntries() << " entries..." << std::endl;
    saved_time = stopwatch.now();
    short_chain->BuildIndex("run", "event");
    current_time = stopwatch.now();
    elapsed_time = current_time - saved_time;
    if (verbose >= 1) std::cout << "Finish building lookup indices with " << short_chain_num_entries << " entries..." << std::endl;
    // print summary
    std::cout << fmt::format("{:=^75}", "SUMMARY: Building Lookup indices") << std::endl;
    std::cout << fmt::format("Total time: {:%T}", elapsed_time) << std::endl;
    std::cout << fmt::format("Average time per entry: {:.05f} ms", elapsed_time.count() * 1000 / short_chain_num_entries) << std::endl;
    std::cout << fmt::format("{:=^75}", "") << std::endl;

    // set up output directory
    if (verbose >= 3) std::cout << "Start preparing output directory..." << std::endl;
    out_directory = (out_directory[out_directory.length()-1] != '/') ? out_directory : out_directory.substr(0, out_directory.length()-1); // remove tailing slash if any
    // std::filesystem::create_directories(out_directory.c_str()); // create output directory if not exist
    if (verbose >= 3) std::cout << "Finish preparing lookup directory..." << std::endl;
    // for testing
    //int max_entries = 100;

    // set up reader
    if (verbose >= 2) std::cout << "Start building treereader..." << std::endl;
    TTreeReader long_chain_reader(long_chain);
    // long_chain_reader.SetEntriesRange(0, max_entries);
    TTreeReaderValue<UInt_t> long_chain_run(long_chain_reader, "run");
    TTreeReaderValue<ULong64_t> long_chain_event_number(long_chain_reader, "event");
    //TTreeReaderValue<Float_t> large_chain_njet(large_chain_reader, "MET_pt");
    if (verbose >= 2) std::cout << "Finish building treereader..." << std::endl;

    TTreeReader short_chain_reader(short_chain);
    TTreeReaderValue<UInt_t> *short_chain_run = new TTreeReaderValue<UInt_t>(short_chain_reader, "run");
    TTreeReaderValue<ULong64_t> *short_chain_event_number = new TTreeReaderValue<ULong64_t>(short_chain_reader, "event");

    // build out_tree_base holding branches
    if (verbose >= 2) std::cout << "Start building output tree..." << std::endl;
    TTree *out_tree_base = new TTree("Events", "Events");
    std::unordered_map<std::string, void*> data_addresses;
    append_branches_from_tree(long_chain, out_tree_base, data_addresses, long_chain_branchname_prefix);
    append_branches_from_tree(short_chain, out_tree_base, data_addresses, short_chain_branchname_prefix);
    if (verbose >= 2) std::cout << "Finish building output tree..." << std::endl;

    // synchronize trees
    Int_t long_chain_saved_tree_number = long_chain->GetTreeNumber();
    Int_t short_chain_saved_tree_number = short_chain->GetTreeNumber();
    // Int_t out_tree_saved_long_chain_current_tree_number = long_chain_current_tree_number;
    // Int_t out_tree_saved_short_chain_current_tree_number = short_chain_current_tree_number;
    // std::cout << "save tree number..." << std::endl;
    
    // clone for running out tree
    auto out_tree = out_tree_base->CloneTree(0);
    // std::cout << "clone..." << std::endl;

    // loop parameter
    Long64_t num_match = 0;
    Long64_t out_tree_current_num_entries = 0;
    Long64_t out_tree_current_size = 0;
    bool is_last_entry = !long_chain_reader.Next();
    Long64_t print_every_entries = Long64_t(print_every_percent * long_chain_num_entries / 100);

    int short_chain_num_entries_num_digits = std::to_string(short_chain_num_entries).length();
    int long_chain_num_entries_num_digits = std::to_string(long_chain_num_entries).length();

    //std::cout << long_chain_run.Get() << short_chain_run->Get() << std::endl;

    //std::cout << get_tree_byte_size(out_tree) << std::endl; 

    // loop over entries and copy over to output tree
    if (verbose >= 1) std::cout << "Start looping over " << long_chain_num_entries << " entries..." << std::endl;
    saved_time = stopwatch.now();
    while(true){
        if(is_last_entry) break;

        // std::cout << long_chain->GetTree()->GetBranch("run")->GetAddress() << std::endl;
        
        Long64_t i_long_chain = long_chain_reader.GetCurrentEntry();
        // search for corresponding event
        Long64_t i_short_chain = short_chain->GetEntryNumberWithIndex(*long_chain_run, *long_chain_event_number);
        
        if (i_short_chain != -1){ // found match
            num_match++; 
            out_tree_current_num_entries++;

            short_chain_reader.SetEntry(i_short_chain);
            //std::cout << std::format("{} {} {} {}", *long_chain_run, **short_chain_run, *long_chain_event_number, **short_chain_event_number)<< std::endl;

            // we might need to re-allocate memory
            Int_t long_chain_current_tree_number = long_chain->GetTreeNumber();
            if (long_chain_current_tree_number != long_chain_saved_tree_number){
                //std::cout << "reallocate long chain " << long_chain_current_tree_number << " " <<  long_chain_saved_tree_number << std::endl;
                reallocate_memory_if_any(long_chain, out_tree_base, data_addresses, long_chain_branchname_prefix);
                reallocate_memory_if_any(long_chain, out_tree, data_addresses, long_chain_branchname_prefix);
                long_chain_saved_tree_number = long_chain_current_tree_number;
            }
            
            Int_t short_chain_current_tree_number = short_chain->GetTreeNumber();
            if (short_chain_current_tree_number != short_chain_saved_tree_number){
                //std::cout << "reallocate short chain " << short_chain_current_tree_number << " " <<  short_chain_saved_tree_number << std::endl;
                reallocate_memory_if_any(short_chain, out_tree_base, data_addresses, short_chain_branchname_prefix);
                reallocate_memory_if_any(short_chain, out_tree, data_addresses, short_chain_branchname_prefix);
                short_chain_saved_tree_number = short_chain_current_tree_number;
            }

            // read all branches for this entry
            long_chain->GetEntry(i_long_chain); 
            short_chain->GetEntry(i_short_chain);
            
            // copy_addresses(short_chain, out_tree, short_chain_branchname_prefix);
            // copy_addresses(long_chain, out_tree, long_chain_branchname_prefix);
            
            // sync_addresses(short_chain, out_tree, short_chain_branchname_prefix);
            // sync_addresses(long_chain, out_tree, long_chain_branchname_prefix);

            // save to output tree
            Int_t num_byte_write = out_tree->Fill();
            if (out_tree_current_num_entries == 1){ // first entry
                out_tree_current_size += get_tree_byte_size(out_tree);
            } else {
                if (num_byte_write > 0) out_tree_current_size += num_byte_write; 
            } 
            //is_last_entry = true;
        }

        if ((verbose >= 1) && (((num_match == 5) && (i_long_chain+1 < print_every_entries)) || ((i_long_chain+1) % print_every_entries) == 0)){
            current_time = stopwatch.now();
            elapsed_time = current_time - saved_time;
            // tqdm style
            //std::cout << std::format("{:06.02f}", elapsed_time.count());
            std::cout << "#process: " << std::setw(long_chain_num_entries_num_digits) << std::left << i_long_chain+1 << "/" << long_chain_num_entries;
            std::cout << std::setw(11) << std::left << fmt::format(" ({:.03f}%)", Double_t(i_long_chain+1)/long_chain_num_entries * 100);
            std::cout << "  #match: " << std::setw(short_chain_num_entries_num_digits) << std::right << num_match; 
            std::cout << fmt::format(" ({:7.03f}%/A, {:7.03f}%/B)", Double_t(num_match)/datasetA_num_entries * 100, Double_t(num_match)/datasetB_num_entries * 100);
            std::cout << fmt::format("   [{:%T}<{:%T}, {:10.02f}it/s, {:10.05f}ms/it]", elapsed_time, elapsed_time/(i_long_chain+1) * long_chain_num_entries - elapsed_time, Double_t(i_long_chain+1)/elapsed_time.count(), elapsed_time.count()/(i_long_chain+1)*1000);
            std::cout << std::endl;
            //std::cout << std::format("Processing entry {} of {} entries ({:03.02f}%) Elapsed Time: {:%T} Average time per entry: {:06.02f}% Projected Remaining Time: {:%T}", i_long_chain+1, long_chain_num_entries, double(i_long_chain+1)/long_chain_num_entries * 100, elapsed_time, elapsed_time.count(), elapsed_time/(i_long_chain+1) * long_chain_num_entries - elapsed_time) << std::endl;
        }
        //long_chain_reader.Next();
        is_last_entry = !long_chain_reader.Next();
        //std::cout << std::format("{} {} {}", out_tree_current_size, get_tree_byte_size(out_tree), get_tree_byte_size(out_tree) - out_tree_current_size) << std::endl;
        
        // current tree is larger than max size, save to file, and reset tree
        if ((out_tree_current_num_entries > 0) && (is_last_entry || (out_tree_current_size > out_tree_max_size))){
            // write to file
            TString out_file_path = TString::Format("%s/%s_%d.root", out_directory.c_str(), out_filename_prefix.c_str(), out_file_index);
            if (verbose >= 2) std::cout << "Saving to file " << out_file_path << std::endl;
            TFile *out_file = TFile::Open(out_file_path.Data(), "RECREATE");
            out_file->cd();
            out_tree->Write();
            out_file->Close();
            
            // reset out_tree
            delete out_tree;
            out_tree = out_tree_base->CloneTree(0);
            out_tree_current_num_entries = 0;
            out_tree_current_size = 0;
    
            // increment file index
            out_file_index++;
        }
    }
    current_time = stopwatch.now();
    elapsed_time = current_time - saved_time;
    if (verbose >= 1) std::cout << "Finishing looping over " << long_chain_num_entries << " entries..." << std::endl;
    
    // print summary
    std::cout << fmt::format("{:=^75}", "SUMMARY: Merging Trees") << std::endl;
    std::cout << fmt::format("Total time: {:%T}", elapsed_time) << std::endl;
    std::cout << fmt::format("Average time per entry: {:.05f} ms", elapsed_time.count() * 1000 / long_chain_num_entries) << std::endl;
    std::cout << "Number of matched events: " << num_match << std::endl;
    std::cout << TString::Format("Percent matched events from datasetA: %lld/%lld (%.03f%%)", num_match, datasetA_num_entries, Double_t(num_match)/datasetA_num_entries * 100) << std::endl;
    std::cout << TString::Format("Percent matched events from datasetB: %lld/%lld (%.03f%%)", num_match, datasetB_num_entries, Double_t(num_match)/datasetB_num_entries * 100) << std::endl;
    std::cout << fmt::format("{:=^75}", "") << std::endl;

    // delete out_tree;
    // delete out_tree_base;
    // delete short_chain;
    // delete long_chain;

    return 0;
}

// helper function implementation

TChain* build_chain(std::string filelist_filename){
    std::ifstream filelist_file(filelist_filename);
    std::string filename;
    TChain* chain = new TChain("Events");
    //num_files
    while (std::getline(filelist_file, filename)){
        chain -> AddFile(filename.c_str());
        //num_files++;
    }
    return chain;
}

// allocate memory
void* allocate_memory_from_leaf(const char* leaf_type_name, bool singleton, Int_t length){
    void* addr;

    if (std::strcmp("Char_t", leaf_type_name) == 0){
        if (singleton) addr = new Char_t;
        else addr = new Char_t[length];
    } else if (std::strcmp("UChar_t", leaf_type_name) == 0){
        if (singleton) addr = new UChar_t;
        else addr = new UChar_t[length];
    } else if (std::strcmp("Short_t", leaf_type_name) == 0){
        if (singleton) addr = new Short_t;
        else addr = new Short_t[length];
    } else if (std::strcmp("UShort_t", leaf_type_name) == 0){
        if (singleton) addr = new UShort_t;
        else addr = new UShort_t[length];
    } else if (std::strcmp("Int_t", leaf_type_name) == 0){
        if (singleton) addr = new Int_t;
        else addr = new Int_t[length];
    } else if (std::strcmp("UInt_t", leaf_type_name) == 0){
        if (singleton) addr = new UInt_t;
        else addr = new UInt_t[length];
    } else if (std::strcmp("Float_t", leaf_type_name) == 0){
        if (singleton) addr = new Float_t;
        else addr = new Float_t[length];
    } else if (std::strcmp("Float16_t", leaf_type_name) == 0){
        if (singleton) addr = new Float16_t;
        else addr = new Float16_t[length];
    } else if (std::strcmp("Double_t", leaf_type_name) == 0){
        if (singleton) addr = new Double_t;
        else addr = new Double_t[length];
    } else if (std::strcmp("Double32_t", leaf_type_name) == 0){
        if (singleton) addr = new Double32_t;
        else addr = new Double32_t[length];
    } else if (std::strcmp("Long64_t", leaf_type_name) == 0){
        if (singleton) addr = new Long64_t;
        else addr = new Long64_t[length];
    } else if (std::strcmp("ULong64_t", leaf_type_name) == 0){
        if (singleton) addr = new ULong64_t;
        else addr = new ULong64_t[length];
    } else if (std::strcmp("Long_t", leaf_type_name) == 0){
        if (singleton) addr = new Long_t;
        else addr = new Long_t[length];
    } else if (std::strcmp("ULong_t", leaf_type_name) == 0){
        if (singleton) addr = new ULong_t();
        else addr = new ULong_t[length];
    } else if (std::strcmp("Bool_t", leaf_type_name) == 0){
        if (singleton) addr = new Bool_t();
        else addr = new Bool_t[length];
    } else {
        throw;
    }
    return addr;
}

void deallocate_memory_from_leaf(const char* leaf_type_name, void* addr){
    if (std::strcmp("Char_t", leaf_type_name) == 0) 
        delete[] (Char_t*)addr;
    else if (std::strcmp("UChar_t", leaf_type_name) == 0) 
        delete[] (UChar_t*)addr;
    else if (std::strcmp("Short_t", leaf_type_name) == 0) 
        delete[] (Short_t*)addr;
    else if (std::strcmp("UShort_t", leaf_type_name) == 0) 
        delete[] (UShort_t*)addr;
    else if (std::strcmp("Int_t", leaf_type_name) == 0) 
        delete[] (Int_t*)addr;
    else if (std::strcmp("UInt_t", leaf_type_name) == 0)
        delete[] (UInt_t*)addr;
    else if (std::strcmp("Float_t", leaf_type_name) == 0)
        delete[] (Float_t*)addr;
    else if (std::strcmp("Float16_t", leaf_type_name) == 0)
        delete[] (Float16_t*)addr;
    else if (std::strcmp("Double_t", leaf_type_name) == 0)
        delete[] (Double_t*)addr;
    else if (std::strcmp("Double32_t", leaf_type_name) == 0)
        delete[] (Double32_t*)addr;
    else if (std::strcmp("Long64_t", leaf_type_name) == 0) 
        delete[] (Long64_t*)addr;
    else if (std::strcmp("ULong64_t", leaf_type_name) == 0)
        delete[] (ULong64_t*)addr;
    else if (std::strcmp("Long_t", leaf_type_name) == 0) 
        delete[] (Long_t*)addr;
    else if (std::strcmp("ULong_t", leaf_type_name) == 0)
        delete[] (ULong_t*)addr;
    else if (std::strcmp("Bool_t", leaf_type_name) == 0) 
        delete[] (Bool_t*)addr;
    else throw;
}

// from TTree::CloneTree and TTree::CopyAddress
void append_branches_from_tree(TTree *src_tree, TTree *dst_tree, std::unordered_map<std::string, void*>& data_addresses, const std::string& prefix){
    //R__COLLECTION_READ_LOCKGUARD(ROOT::gCoreMutex);
    
    TTree* this_tree = src_tree->GetTree(); // if src_tree is a TChain, this get the current tree
    
    // loop over branches
    TObjArray* src_branches = this_tree->GetListOfBranches();
    Int_t num_src_branches = src_branches->GetEntriesFast();

    // deal with branches
    for (Int_t i_src_branch = 0; i_src_branch < num_src_branches; ++i_src_branch) {
        TBranch* src_branch = (TBranch*)(src_branches->At(i_src_branch));
        //if (!this_tree->GetBranchStatus(src_branch->GetName())) continue; // skip inactive branch
        if (src_branch->TestBit(kDoNotProcess)) continue;

        const char *src_branch_name = src_branch->GetName();
        //if (src_branch_name.EqualTo("run") || src_branch_name.EqualTo("event")) continue;

        // formulate dst_branch_name
        size_t prefix_length = std::strlen(prefix.c_str());
        size_t src_branch_name_length = std::strlen(src_branch_name);
        char *dst_branch_name = new char[prefix_length + src_branch_name_length + 1];
        if (src_branch_name[0] == 'n'){
            dst_branch_name[0] = 'n';
            for (size_t i = 0; i < prefix_length; ++i) 
                dst_branch_name[1 + i] = prefix[i];
            for (size_t i = 0; i < src_branch_name_length - 1; ++i) 
                dst_branch_name[1 + prefix_length + i] = src_branch_name[1 + i];
            dst_branch_name[prefix_length + src_branch_name_length] = '\0';
        } else {
            for (size_t i = 0; i < prefix_length; ++i) 
                dst_branch_name[i] = prefix[i];
            for (size_t i = 0; i < src_branch_name_length; ++i) 
                dst_branch_name[prefix_length + i] = src_branch_name[i];
            dst_branch_name[prefix_length + src_branch_name_length] = '\0';
        }
        //std::cout << src_branch_name << " " << dst_branch_name << std::endl;

        // // //TDirectory* ndir = dst_branch
        // // TObjArray* src_leaves = src_branch->GetListOfLeaves();
        // // std::cout << "src: " << src_branch->GetName() << " " << src_branch->GetTitle() << " " << src_branch->GetFullName() ;
        // // // for(Int_t i_src_leaf = 0; i_src_leaf < src_leaves->GetEntriesFast(); ++i_src_leaf){
        // // //     TLeaf* src_leaf = (TLeaf*)src_leaves->At(i_src_leaf);
        // // //     //std::cout << src_leaf->GetName() << ", ";
        // // // }
        // // std::cout << std::endl;
        
        // // add dst_branch to dst_tree
        // dst_tree->GetListOfBranches()->Add(dst_branch);

        // std::cout << "dst: " << dst_branch->GetName() << " " << dst_branch->GetTitle() << " " << dst_branch->GetFullName() ;
        // // add dst_leaves also to dst_tree
        // TObjArray* dst_leaves = dst_branch->GetListOfLeaves();
        // Int_t dst_nleaves = dst_leaves->GetEntriesFast();
        // for(Int_t i_dst_leaf = 0; i_dst_leaf < dst_nleaves; ++i_dst_leaf){
        //     TLeaf* dst_leaf = (TLeaf*)dst_leaves->At(i_dst_leaf);
        //     //std::cout << dst_leaf->GetName() << ", ";
        //     dst_tree->GetListOfLeaves()->Add(dst_leaf);
        // }

        // std::cout << std::endl;

        // get leaf
        TLeaf* src_leaf = (TLeaf*) src_branch->GetListOfLeaves()->At(0);
        const char *src_leaf_type_name = src_leaf->GetTypeName();
        char src_leaf_type;

        // if (branch->GetNleaves() != 1) should throw for nanoAOD
        // std::cout << branch_name << "num leaves: " << branch->GetNleaves() << std::endl;
        
        
        //TLeaf *src_leaf = (TLeaf*)(src_branch->GetListOfLeaves()->At(0)); 
        //std::cout << src_first_leaf->GetValuePointer() << std::endl;
        
        //bool singleton = ((src_leaf->GetLen()!=0) && (!src_leaf->GetLeafCount())); // do not have counter leaf
        bool singleton = (!src_leaf->GetLeafCount());
        Int_t length = 1;
        if (!singleton){
            length = src_leaf->GetLeafCount()->GetMaximum();
            std::cout << "maximum: " << length << std::endl;
        }

        // address to hold data on heap, so you're responsible to remove them
        void *data_addr = allocate_memory_from_leaf(src_leaf_type_name, singleton, length); 

        // resolve type for leaf list
        if (std::strcmp("Char_t", src_leaf_type_name) == 0)
            src_leaf_type = 'B';
        else if (std::strcmp("UChar_t", src_leaf_type_name) == 0)
            src_leaf_type = 'b';
        else if (std::strcmp("Short_t", src_leaf_type_name) == 0)
            src_leaf_type = 'S';
        else if (std::strcmp("UShort_t", src_leaf_type_name) == 0)
            src_leaf_type = 's';
        else if (std::strcmp("Int_t", src_leaf_type_name) == 0)
            src_leaf_type = 'I';
        else if (std::strcmp("UInt_t", src_leaf_type_name) == 0)
            src_leaf_type = 'i';
        else if (std::strcmp("Float_t", src_leaf_type_name) == 0)
            src_leaf_type = 'F';
        else if (std::strcmp("Float16_t", src_leaf_type_name) == 0)
            src_leaf_type = 'f';
        else if (std::strcmp("Double_t", src_leaf_type_name) == 0)
            src_leaf_type = 'D';
        else if (std::strcmp("Double32_t", src_leaf_type_name) == 0)
            src_leaf_type = 'd';
        else if (std::strcmp("Long64_t", src_leaf_type_name) == 0)
            src_leaf_type = 'L';
        else if (std::strcmp("ULong64_t", src_leaf_type_name) == 0)
            src_leaf_type = 'l';
        else if (std::strcmp("Long_t", src_leaf_type_name) == 0)
            src_leaf_type = 'G';
        else if (std::strcmp("ULong_t", src_leaf_type_name) == 0)
            src_leaf_type = 'g';
        else if (std::strcmp("Bool_t", src_leaf_type_name) == 0)
            src_leaf_type = 'O';
        else
            throw;
        
        // formualte leaflist for dst_tree
        char* dst_leaf_list;
        size_t dst_branch_name_length = std::strlen(dst_branch_name);
        if (singleton){ // singleton
            dst_leaf_list = new char[dst_branch_name_length + 3];
            for (size_t i = 0; i < dst_branch_name_length; ++i)
                dst_leaf_list[i] = dst_branch_name[i];
            dst_leaf_list[dst_branch_name_length] = '/';
            dst_leaf_list[dst_branch_name_length + 1] = src_leaf_type;
            dst_leaf_list[dst_branch_name_length + 2] = '\0';
        } else {
            const char *src_branch_count_name = src_leaf->GetLeafCount()->GetName();
            size_t src_branch_count_name_length = std::strlen(src_branch_count_name);
            dst_leaf_list = new char[dst_branch_name_length + src_branch_count_name_length + prefix_length + 5];
            std::cout << "leaf count: " << src_branch_count_name << std::endl;
            for (size_t i = 0; i < dst_branch_name_length; ++i)
                dst_leaf_list[i] = dst_branch_name[i];
            dst_leaf_list[dst_branch_name_length] = '[';
            dst_leaf_list[dst_branch_name_length + 1] = 'n';
            for (size_t i = 0; i < prefix_length; ++i)
                dst_leaf_list[dst_branch_name_length + 2 + i] = prefix[i];
            for (size_t i = 0; i < src_branch_count_name_length - 1; ++i)
                dst_leaf_list[dst_branch_name_length + 2 + prefix_length + i] = src_branch_count_name[1 + i];
            dst_leaf_list[dst_branch_name_length + 2 + prefix_length + src_branch_count_name_length - 1] = ']';
            dst_leaf_list[dst_branch_name_length + 2 + prefix_length + src_branch_count_name_length] = '/';
            dst_leaf_list[dst_branch_name_length + 2 + prefix_length + src_branch_count_name_length + 1] = src_leaf_type;
            dst_leaf_list[dst_branch_name_length + 2 + prefix_length + src_branch_count_name_length + 2] = '\0';
        }

        //std::cout << "dst_leaf_list: " << dst_leaf_list << std::endl;
        
        // set src branch address with data_addr
        src_tree->SetBranchAddress(src_branch_name, data_addr);

        // create dst branch with same data_addr
        TBranch *dst_branch = dst_tree->Branch(dst_branch_name, data_addr, dst_leaf_list);
        dst_branch->SetTitle(src_branch->GetTitle()); // copy over doc

        // if it is a counter leaf, set maximum
        if (src_branch_name[0] == 'n'){ 
            ((TLeaf*)(dst_branch->GetListOfLeaves()->At(0)))->IncludeRange(src_leaf);
            //((TLeaf*)(dst_branch->GetListOfLeaves()->At(0)))->SetMaximum(length);
        }

        // std::cout << "src: " << src_branch->GetName() << " " << src_branch->GetTitle() << " " << src_branch->GetFullName() << std::endl;
        // std::cout << "dst: " << dst_branch->GetName() << " " << dst_branch->GetTitle() << " " << dst_branch->GetFullName() << std::endl;
        // std::cout << src_first_leaf->GetValuePointer() << std::endl;
        //std::cout << "addr: " << branch_addr << " : "<< ((TLeaf*)(src_tree->GetBranch(src_branch_name)->GetListOfLeaves()->At(0)))->GetValuePointer() << " : " << ((TLeaf*)(dst_tree->GetBranch(dst_branch_name)->GetListOfLeaves()->At(0)))->GetValuePointer() << std::endl;

        // std::cout << "addr: " << branch_addr << std::endl;
        // TBranch* sbr = src_tree->GetBranch(src_branch_name);
        // if (sbr){
        //     std::cout << "Find src branch ";
        //     if (sbr->GetAddress()) std::cout << "Get Address not null " << std::endl;
        //     if (((TLeaf*)(sbr->GetListOfLeaves()->At(0)))->GetValuePointer()) std::cout << "First leaf not null " << ((TLeaf*)(sbr->GetListOfLeaves()->At(0)))->GetValuePointer() << std::endl;
        // } else {
        //     std::cout << "Cannot find src branch";
        // }
        // std::cout << std::endl;

        // TBranch* dbr = dst_tree->GetBranch(dst_branch_name);
        // if (dbr){
        //     std::cout << "Find dst branch ";
        //     if (dbr->GetAddress()) std::cout << "Get Address not null " << std::endl;
        //     if (((TLeaf*)(dbr->GetListOfLeaves()->At(0)))->GetValuePointer()) std::cout << "First leaf not null " << ((TLeaf*)(dbr->GetListOfLeaves()->At(0)))->GetValuePointer() << std::endl;
        // } else {
        //     std::cout << "Cannot find dst branch";
        // }
        // std::cout << std::endl;

        // save data addresses
        data_addresses[std::string(dst_branch->GetName())] = data_addr;
    }
}

void reallocate_memory_if_any(TTree *src_tree, TTree *dst_tree, std::unordered_map<std::string, void*>& data_addresses, const std::string& prefix){
    //R__COLLECTION_READ_LOCKGUARD(ROOT::gCoreMutex);
    
    TTree* this_tree = src_tree->GetTree(); // if src_tree is a TChain, this get the current tree

    std::set<TLeaf*> updated_leaf_count;
    
    // loop over branches
    TObjArray* src_branches = this_tree->GetListOfBranches();
    Int_t num_src_branches = src_branches->GetEntriesFast();
    //TObjArray* dst_branches = dst_tree->GetListOfBranches();
    //Int_t num_dst_branches = dst_branches->GetEntriesFast();
    
    for (Int_t i_src_branch = 0; i_src_branch < num_src_branches; ++i_src_branch) {
        //std::cout << i_src_branch << " ";
        TBranch* src_branch = (TBranch*)(src_branches->At(i_src_branch));
        if (src_branch->TestBit(kDoNotProcess)) continue; // skip inactive branch

        // formulate branch names
        //TString src_branch_name(src_branch->GetName());
        //TString dst_branch_name;
        //if (src_branch_name.BeginsWith("n")){
            //dst_branch_name = TString("n") + prefix + src_branch_name(1, src_branch_name.Length());
        //} else {
        //} prefix + src_branch_name : 

        // get src leaf
        TLeaf* src_leaf = (TLeaf*) (src_branch->GetListOfLeaves()->At(0));
        bool singleton = (!src_leaf->GetLeafCount()); // do not have counter leaf
        //std::cout << src_branch_name << " " << dst_branch_name << " singleton: " << singleton << std::endl;
        //std::cout << "before continue" << std::endl;
        if (singleton) continue; // if singleton, do not need to reallocate
        //std::cout << "after continue" << std::endl;

        // get dst branch and leaf
        // TBranch* dst_branch = nullptr;
        // TLeaf* dst_leaf = nullptr;
        // std::cout << src_branch->GetAddress() << std::endl;
        // for (Int_t i_dst_branch = 0; i_dst_branch < num_dst_branches; ++i_dst_branch){
        //     TBranch* br = (TBranch*)(dst_branches->At(i_dst_branch));
        //     if (src_branch->GetAddress() == br->GetAddress()){ // assume same get address
        //         dst_branch = br;
        //         dst_leaf = (TLeaf*) (br->GetListOfLeaves()->At(0));
        //         break;
        //     }
        // }
        // if (!dst_branch || !dst_leaf) {
        //     std::cout << "dst branch not found " << std::endl;
        // }
        // if (!dst_leaf) {
        //     std::cout << "dst leaf not found " << std::endl;
        // }
        // //TLeaf*  = (TLeaf*) (dst_branch->GetListOfLeaves()->At(0));

        // std::cout << src_branch->GetName() << " " << dst_branch->GetName() << std::endl;

        // formulate dst_branch_name
        const char* src_branch_name = src_branch->GetName();
        size_t prefix_length = std::strlen(prefix.c_str());
        size_t src_branch_name_length = std::strlen(src_branch_name);
        char *dst_branch_name = new char[prefix_length + src_branch_name_length + 1];
        if (src_branch_name[0] == 'n'){
            dst_branch_name[0] = 'n';
            for (size_t i = 0; i < prefix_length; ++i) 
                dst_branch_name[1 + i] = prefix[i];
            for (size_t i = 0; i < src_branch_name_length - 1; ++i) 
                dst_branch_name[1 + prefix_length + i] = src_branch_name[1 + i];
            dst_branch_name[prefix_length + src_branch_name_length] = '\0';
        } else {
            for (size_t i = 0; i < prefix_length; ++i) 
                dst_branch_name[i] = prefix[i];
            for (size_t i = 0; i < src_branch_name_length; ++i) 
                dst_branch_name[prefix_length + i] = src_branch_name[i];
            dst_branch_name[prefix_length + src_branch_name_length] = '\0';
        }

        //std::cout << dst_branch_name << std::endl;
        TBranch* dst_branch = dst_tree->GetBranch(dst_branch_name);
        TLeaf* dst_leaf = (TLeaf*)(dst_branch->GetListOfLeaves()->At(0));
        //std::cout << src_branch->GetName() << " " << dst_branch->GetName() << std::endl;
        
        if (updated_leaf_count.find(dst_leaf->GetLeafCount()) != updated_leaf_count.end()){ // counter was changed by other leaves
            const char *src_leaf_type_name = src_leaf->GetTypeName();
            Int_t length = dst_leaf->GetLeafCount()->GetMaximum();
            void* new_data_addr = allocate_memory_from_leaf(src_leaf_type_name, false, length);
            void* old_data_addr = data_addresses[std::string(dst_branch->GetName())];

            src_tree->SetBranchAddress(src_branch->GetName(), new_data_addr);
            dst_tree->SetBranchAddress(dst_branch->GetName(), new_data_addr);
            
            // save new addresses
            data_addresses[std::string(dst_branch->GetName())] = new_data_addr;

            // free old addresses
            deallocate_memory_from_leaf(src_leaf_type_name, old_data_addr);
            
        } else {
            Int_t src_max_length = src_leaf->GetLeafCount()->GetMaximum();
            Int_t dst_max_length = dst_leaf->GetLeafCount()->GetMaximum();

            if (dst_max_length < src_max_length){ // need to reset address
                const char *src_leaf_type_name = src_leaf->GetTypeName();
                
                void* new_data_addr = allocate_memory_from_leaf(src_leaf_type_name, false, src_max_length);
                void* old_data_addr = data_addresses[std::string(dst_branch->GetName())];
        
                src_tree->SetBranchAddress(src_branch->GetName(), new_data_addr);
                dst_tree->SetBranchAddress(dst_branch->GetName(), new_data_addr);
        
                dst_leaf->GetLeafCount()->IncludeRange(src_leaf->GetLeafCount());

                updated_leaf_count.insert(dst_leaf->GetLeafCount());

                // save new addresses
                data_addresses[std::string(dst_branch->GetName())] = new_data_addr;
    
                // free old addresses
                deallocate_memory_from_leaf(src_leaf_type_name, old_data_addr);
            }
        }
        // deallocate dst_branch_name
        delete[] dst_branch_name;
    }
    //delete updated_leaf_count;
}

Long64_t get_tree_byte_size(TTree* tree){
    // from TTree::ChangeFile
    TBufferFile b(TBuffer::kWrite, 10000); // bufsize=10000, this is hard-coded in ROOT
    TTree::Class()->WriteBuffer(b, tree);
    return b.Length();
}
