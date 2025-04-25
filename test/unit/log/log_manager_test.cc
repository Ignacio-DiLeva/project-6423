#include <gtest/gtest.h>
#include <string>

#include "heap/heap_file.h"
#include "log/log_manager.h"
#include "transaction/transaction_manager.h"
#include "common/macros.h"
#include "buffer/buffer_manager.h"
#include "storage/test_file.h"


using buzzdb::BufferManager;
using buzzdb::LogManager;
using buzzdb::HeapSegment;
using buzzdb::TransactionManager;
using buzzdb::TID;
using buzzdb::BufferFrame;
using buzzdb::SlottedPage;
using buzzdb::File;

using buzzdb::INVALID_FIELD;

const char* LOG_FILE = buzzdb::LOG_FILE_PATH.c_str();
constexpr uint64_t HEAP_SEGMENT = 123;

namespace {

class LogManagerTest: public ::testing::Test{
	void SetUp() { 
		auto file_handle = File::open_file(std::to_string(HEAP_SEGMENT).c_str(), 
	   										File::WRITE);
		file_handle->resize(0);
       	auto log_handle = File::open_file(LOG_FILE, File::WRITE);
		log_handle->resize(0);
   }
};

TID insert_row(HeapSegment& heap_segment,
		TransactionManager& transaction_manager,
		UNUSED_ATTRIBUTE uint64_t txn_id,
		uint64_t table_id, uint64_t field){

	auto tuple_size = sizeof(uint64_t)*2; // table_id | field

	// Allocate slot
	auto tid = heap_segment.allocate(tuple_size);

	// Write buffer
	std::vector<char> buf;
	buf.resize(tuple_size);
	memcpy(buf.data() + 0, &table_id, sizeof(uint64_t));
	memcpy(buf.data() + sizeof(uint64_t), &field, sizeof(uint64_t));

	heap_segment.write(tid, reinterpret_cast<std::byte *>(buf.data()),
			tuple_size, txn_id);

	uint64_t page_id = tid.value >> 16;
	uint64_t overall_page_id =
		BufferManager::get_overall_page_id(heap_segment.segment_id_, page_id);
	transaction_manager.add_modified_page(txn_id, overall_page_id);
	return tid;
}

// Check whether the specified tuple is, or is not, present
bool look(HeapSegment& heap_segment,
		UNUSED_ATTRIBUTE TransactionManager& transaction_manager,
		BufferManager& buffer_manager,
		UNUSED_ATTRIBUTE uint64_t table_id, uint64_t expected_field,
		bool should_be_present){

	auto tuple_size = sizeof(uint64_t)*2; // table_id | field
	size_t count = 0;

	// Go over all pages
	for (size_t segment_page_itr = 0;
			segment_page_itr < heap_segment.page_count_;
			segment_page_itr++) {

			uint64_t page_id =
					BufferManager::get_overall_page_id(
							heap_segment.segment_id_, segment_page_itr);

			BufferFrame &frame = buffer_manager.fix_page(page_id, true);

			auto* page = reinterpret_cast<SlottedPage*>(frame.get_data());
			page->header.buffer_frame = reinterpret_cast<char*>(page);
			auto overall_page_id = page->header.overall_page_id;
			auto slot_count = page->header.first_free_slot;

			// Go over all slots in page
			for(size_t slot_itr = 0;
					slot_itr < slot_count;
					slot_itr++){
				TID tid = TID(overall_page_id, slot_itr);

				// Check slot
				std::vector<char> buf;
				buf.resize(tuple_size);
				uint64_t table_id, field;

				heap_segment.read(tid, reinterpret_cast<std::byte *>(buf.data()),
						tuple_size);

				memcpy(&table_id, buf.data() + 0, sizeof(uint64_t));
				memcpy(&field, buf.data() + sizeof(uint64_t), sizeof(uint64_t));

				if(field == expected_field){
					count = count + 1;
				}
			}

			buffer_manager.unfix_page(frame, true);
		}

	// tuple repeated
	if(count > 1){
		return false;
	}
	// tuple missing
	else if(count == 0 && should_be_present == true){
		return false;
	}
	// tuple missing
	else if(count > 0 && should_be_present == false){
		return false;
	}

	return true;
}

void do_insert(HeapSegment& heap_segment,
		TransactionManager& transaction_manager,
		BufferManager& buffer_manager,
		uint64_t table_id, uint64_t field_1, uint64_t field_2){
	auto txn_id = transaction_manager.start_txn();
	
	if(field_1 != INVALID_FIELD){
		insert_row(heap_segment, transaction_manager,txn_id, table_id, field_1);
	}

	buffer_manager.flush_all_pages();

	if(field_2 != INVALID_FIELD){
		insert_row(heap_segment, transaction_manager ,txn_id, table_id, field_2);
	}

	transaction_manager.commit_txn(txn_id);
}

void abort(TransactionManager& transaction_manager,
		BufferManager& buffer_manager,
		uint64_t txn_id){
	buffer_manager.flush_all_pages(); // DEFEAT NO-STEAL
	transaction_manager.abort_txn(txn_id);	
}

/* Insert tuples
 * Force dirty pages to disk, defeating NO-STEAL
 * abort
*/
void dont_insert(HeapSegment& heap_segment,
		TransactionManager& transaction_manager,
		BufferManager& buffer_manager,
		uint64_t table_id, uint64_t field_1, uint64_t field_2){
	
	auto txn_id = transaction_manager.start_txn();

	if(field_1 != INVALID_FIELD){
		insert_row(heap_segment, transaction_manager, txn_id, table_id, field_1);
	}

	
	if(field_2 != INVALID_FIELD){
		insert_row(heap_segment, transaction_manager, txn_id, table_id, field_2);
	}


	abort(transaction_manager, buffer_manager, txn_id);
}

/* Simulate crash
 * Reset DB
 * recovery
*/

void crash(TransactionManager& transaction_manager,
		BufferManager& buffer_manager, 
		LogManager log_manager){
	buffer_manager.discard_all_pages();
	auto log_file = buzzdb::File::open_file(LOG_FILE, buzzdb::File::WRITE);
	log_manager.reset(log_file.get());
	transaction_manager.reset(log_manager);
	log_manager.recovery(buffer_manager);
}


TEST_F(LogManagerTest, LogRecordTest) {
	BufferManager buffer_manager(128, 10);
	auto logfile = buzzdb::File::open_file(LOG_FILE, buzzdb::File::WRITE);
	LogManager log_manager(logfile.get());
	HeapSegment heap_segment(123, log_manager, buffer_manager);
	TransactionManager transaction_manager(log_manager, buffer_manager);

	uint64_t table_id = 101;
	do_insert(heap_segment, transaction_manager, buffer_manager,
			table_id, 5, 10);
	
	// check number of log records
    EXPECT_EQ(log_manager.get_total_log_records(), 4);

	// check number of update log records
    EXPECT_EQ(log_manager.get_total_log_records_of_type(
    		LogManager::LogRecordType::UPDATE_RECORD), 2);
}

TEST_F(LogManagerTest, FlushAllTest) {
	BufferManager buffer_manager(128, 10);
	auto logfile = buzzdb::File::open_file(LOG_FILE, buzzdb::File::WRITE);
	LogManager log_manager(logfile.get());
	HeapSegment heap_segment(123, log_manager, buffer_manager);
	TransactionManager transaction_manager(log_manager, buffer_manager);

	uint64_t table_id = 101;
	do_insert(heap_segment, transaction_manager, buffer_manager,
			table_id, 5, 10);

	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 5, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 10, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 3, false));

	auto txn_id = transaction_manager.start_txn();

	insert_row(heap_segment, transaction_manager, table_id, txn_id, 3);

	buffer_manager.flush_all_pages();

	buffer_manager.discard_all_pages();

	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 3, true));

}

/* insert, crash and recover: data should be consistent
*/
TEST_F(LogManagerTest, TestCommitCrash){
	BufferManager buffer_manager(128, 10);
	auto logfile = buzzdb::File::open_file(LOG_FILE, buzzdb::File::WRITE);
	LogManager log_manager(logfile.get());
	HeapSegment heap_segment(123, log_manager, buffer_manager);
	TransactionManager transaction_manager(log_manager, buffer_manager);
	
	
	uint64_t table_id = 101;
	do_insert(heap_segment, transaction_manager, buffer_manager,
			table_id, 5, 10);

	crash(transaction_manager, buffer_manager, log_manager);

	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 5, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 10, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 3, false));

} 


/* insert, abort: data should not be there
 * flush pages directly to the heap file to defeat NO-STEAL policy	
*/
TEST_F(LogManagerTest, TestAbort){
	BufferManager buffer_manager(128, 10);
	auto logfile = buzzdb::File::open_file(LOG_FILE, buzzdb::File::WRITE);
	LogManager log_manager(logfile.get());
	HeapSegment heap_segment(123, log_manager, buffer_manager);
	TransactionManager transaction_manager(log_manager, buffer_manager);
	

	uint64_t table_id = 101;
	
	do_insert(heap_segment, transaction_manager, buffer_manager,
			table_id, 5, 10);

	dont_insert(heap_segment, transaction_manager, buffer_manager, table_id, 3, 4);

	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 5, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 10, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 3, false));	
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 4, false));

}


/** T1 start, T2 start and commit, T1
*/
TEST_F(LogManagerTest, TestAbortCommitInterleaved){
	BufferManager buffer_manager(128, 10);
	auto logfile = buzzdb::File::open_file(LOG_FILE, buzzdb::File::WRITE);
	LogManager log_manager(logfile.get());
	HeapSegment heap_segment(HEAP_SEGMENT, log_manager, buffer_manager);
	TransactionManager transaction_manager(log_manager, buffer_manager);
	
	
	uint64_t table_id = 101;
	uint64_t txn_1 = transaction_manager.start_txn();
	
	insert_row(heap_segment, transaction_manager, txn_1, table_id, 5);
	
	uint64_t txn_2 = transaction_manager.start_txn();

	insert_row(heap_segment, transaction_manager, txn_2, table_id, 3);
	insert_row(heap_segment, transaction_manager, txn_2, table_id, 4);

	transaction_manager.commit_txn(txn_2);

	insert_row(heap_segment, transaction_manager, txn_1, table_id, 10);
	abort(transaction_manager, buffer_manager, txn_1);



	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 3, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 4, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 5, false));	
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 10, false));
	

}

/*
	Test: abort, crash and recover
*/
TEST_F(LogManagerTest, TestAbortCrash){
	BufferManager buffer_manager(128, 10);
	auto logfile = buzzdb::File::open_file(LOG_FILE, buzzdb::File::WRITE);
	LogManager log_manager(logfile.get());
	HeapSegment heap_segment(123, log_manager, buffer_manager);
	TransactionManager transaction_manager(log_manager, buffer_manager);
	
	uint64_t table_id = 101;
	do_insert(heap_segment, transaction_manager, buffer_manager,
			table_id, 5, 10);
	
	dont_insert(heap_segment, transaction_manager, buffer_manager, table_id, 3, 4);

	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 5, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 10, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 3, false));	
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 4, false));

	crash(transaction_manager, buffer_manager, log_manager);
	
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 5, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 10, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 3, false));	
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 4, false));
}

/** 
 * T1 inserts and commits
 * T2 inserts and aborts
 * T3 inserts and commits
 * Only T1 and T3 data should be there
*/
TEST_F(LogManagerTest, TestCommitAbortCommitCrash){
	BufferManager buffer_manager(128, 10);
	auto logfile = buzzdb::File::open_file(LOG_FILE, buzzdb::File::WRITE);
	LogManager log_manager(logfile.get());
	HeapSegment heap_segment(123, log_manager, buffer_manager);
	TransactionManager transaction_manager(log_manager, buffer_manager);

	uint64_t table_id = 101;
	do_insert(heap_segment, transaction_manager, buffer_manager,
			table_id, 5, 10);

	dont_insert(heap_segment, transaction_manager, buffer_manager, table_id, 3, 4);

	do_insert(heap_segment, transaction_manager, buffer_manager,
			table_id, 1, 2);

	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 5, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 10, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 3, false));	
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 4, false));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 1, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 2, true));
	

	// crash

	crash(transaction_manager, buffer_manager, log_manager);

	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 5, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 10, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 3, false));	
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 4, false));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 1, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 2, true));

}

/** 
 * insert but no commit
 * crash
 * data should not be there
*/

TEST_F(LogManagerTest, TestOpenCrash){
	BufferManager buffer_manager(128, 10);
	auto logfile = buzzdb::File::open_file(LOG_FILE, buzzdb::File::WRITE);
	LogManager log_manager(logfile.get());
	HeapSegment heap_segment(123, log_manager, buffer_manager);
	TransactionManager transaction_manager(log_manager, buffer_manager);

	uint64_t table_id = 101;

	uint64_t txn_id =transaction_manager.start_txn();

	insert_row(heap_segment, transaction_manager, txn_id, table_id, 5);
	
	buffer_manager.flush_all_pages(); // requires undo

	insert_row(heap_segment, transaction_manager, txn_id, table_id, 10);
	
	crash(transaction_manager, buffer_manager, log_manager);

	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 5, false));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 10, false));

}

/** 
 * T1 inserts but does not commits
 * T2 inserts and commits
 * T3 inserts but does not commits
 * crash
 * Only T2 data should be there
*/

TEST_F(LogManagerTest, TestOpenCommitOpenCrash){
	BufferManager buffer_manager(128, 10);
	auto logfile = buzzdb::File::open_file(LOG_FILE, buzzdb::File::WRITE);
	LogManager log_manager(logfile.get());
	HeapSegment heap_segment(123, log_manager, buffer_manager);
	TransactionManager transaction_manager(log_manager, buffer_manager);

	uint64_t table_id = 101;
	// T1
	uint64_t t1 = transaction_manager.start_txn();
	insert_row(heap_segment, transaction_manager, t1, table_id, 5);
	buffer_manager.flush_all_pages(); // defeat no steal 
	// T2
	do_insert(heap_segment, transaction_manager, buffer_manager, table_id, 3, 4);

	// T3
	uint64_t t3 = transaction_manager.start_txn();
	insert_row(heap_segment, transaction_manager, t3, table_id, 10);
	buffer_manager.flush_all_pages(); // defeat no steal

	crash(transaction_manager, buffer_manager, log_manager);

	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 5, false));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 10, false));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 3, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 4, true));

}


/** 
 * T1 inserts but does not commits
 * T2 inserts and commits
 * checkpoint
 * T3 inserts but does not commits
 * crash
 * Only T2 data should be there
*/

TEST_F(LogManagerTest, TestOpenCommitCheckpointOpenCrash){
	BufferManager buffer_manager(128, 10);
	auto logfile = buzzdb::File::open_file(LOG_FILE, buzzdb::File::WRITE);
	LogManager log_manager(logfile.get());
	HeapSegment heap_segment(123, log_manager, buffer_manager);
	TransactionManager transaction_manager(log_manager, buffer_manager);
	
	uint64_t table_id = 101;
	// T1
	uint64_t t1 = transaction_manager.start_txn();
	insert_row(heap_segment, transaction_manager, t1, table_id, 5);
	buffer_manager.flush_all_pages(); // defeat no steal 
	// T2
	do_insert(heap_segment, transaction_manager, buffer_manager, table_id, 3, 4);

	log_manager.log_checkpoint(buffer_manager);

	// T3
	uint64_t t3 = transaction_manager.start_txn();
	insert_row(heap_segment, transaction_manager, t3, table_id, 10);
	buffer_manager.flush_all_pages(); // defeat no steal

	crash(transaction_manager, buffer_manager, log_manager);

	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 5, false));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 10, false));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 3, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 4, true));

}

/** 
 * T1 inserts but does not commits
 * T2 inserts but does not commits
 * T3 inserts but does not commits
 * T4 inserts but does not commits
 * T3 commits
 * fuzzy checkpoint begin
 * fuzzy checkpoint do step
 * T1 commits
 * T4 inserts but does not commits
 * T4 inserts but does not commits
 * fuzzy checkpoint do step
 * fuzzy checkpoint end
 * T2 inserts but does not commits
 * T2 commits
 * crash
 * Only T1,T2,T3 data should be there
*/
TEST_F(LogManagerTest, TestFuzzyCheckpointCompletesThenCrash){
	BufferManager buffer_manager(128, 10);
	auto logfile = buzzdb::File::open_file(LOG_FILE, buzzdb::File::WRITE);
	LogManager log_manager(logfile.get());
	HeapSegment heap_segment1(123, log_manager, buffer_manager);
	HeapSegment heap_segment2(124, log_manager, buffer_manager);
	TransactionManager transaction_manager(log_manager, buffer_manager);
	
	uint64_t table_id1 = 101;
	uint64_t table_id2 = 102;
	// T1
	uint64_t t1 = transaction_manager.start_txn();
	insert_row(heap_segment1, transaction_manager, t1, table_id1, 5);
	// T2
	uint64_t t2 = transaction_manager.start_txn();
	insert_row(heap_segment2, transaction_manager, t2, table_id2, 4);
	
	// T3
	uint64_t t3 = transaction_manager.start_txn();
	insert_row(heap_segment2, transaction_manager, t3, table_id2, 3);
	transaction_manager.commit_txn(t3);

	// T4
	uint64_t t4 = transaction_manager.start_txn();
	insert_row(heap_segment1, transaction_manager, t4, table_id1, 9);

	size_t num_dirty = log_manager.log_fuzzy_checkpoint_begin(buffer_manager);
	EXPECT_TRUE(num_dirty == 2);
	log_manager.log_fuzzy_checkpoint_do_step(buffer_manager, 0);
	transaction_manager.commit_txn(t1);

	// T4
	insert_row(heap_segment1, transaction_manager, t4, table_id1, 10);
	insert_row(heap_segment2, transaction_manager, t4, table_id2, 11);
	
	log_manager.log_fuzzy_checkpoint_do_step(buffer_manager, 1);
	log_manager.log_fuzzy_checkpoint_end();

	insert_row(heap_segment1, transaction_manager, t2, table_id1, 8);

	transaction_manager.commit_txn(t2);

	EXPECT_EQ(log_manager.get_total_log_records(), 16);
	EXPECT_EQ(log_manager.get_total_log_records_of_type(LogManager::LogRecordType::BEGIN_RECORD), 4);
	EXPECT_EQ(log_manager.get_total_log_records_of_type(LogManager::LogRecordType::UPDATE_RECORD), 7);
	EXPECT_EQ(log_manager.get_total_log_records_of_type(LogManager::LogRecordType::COMMIT_RECORD), 3);
	EXPECT_EQ(log_manager.get_total_log_records_of_type(LogManager::LogRecordType::BEGIN_FUZZY_CHECKPOINT_RECORD), 1);
	EXPECT_EQ(log_manager.get_total_log_records_of_type(LogManager::LogRecordType::END_FUZZY_CHECKPOINT_RECORD), 1);
	EXPECT_EQ(log_manager.get_total_log_records_of_type(LogManager::LogRecordType::CHECKPOINT_RECORD), 0);
	EXPECT_EQ(log_manager.get_total_log_records_of_type(LogManager::LogRecordType::ABORT_RECORD), 0);

	crash(transaction_manager, buffer_manager, log_manager);

	EXPECT_TRUE(look(heap_segment2, transaction_manager, buffer_manager,
		table_id2, 3, true));
	EXPECT_TRUE(look(heap_segment2, transaction_manager, buffer_manager,
		table_id2, 4, true));
	EXPECT_TRUE(look(heap_segment1, transaction_manager, buffer_manager,
			table_id1, 5, true));
	EXPECT_TRUE(look(heap_segment1, transaction_manager, buffer_manager,
		table_id1, 8, true));
	EXPECT_TRUE(look(heap_segment1, transaction_manager, buffer_manager,
			table_id1, 9, false));
	EXPECT_TRUE(look(heap_segment1, transaction_manager, buffer_manager,
		table_id1, 10, false));
	EXPECT_TRUE(look(heap_segment2, transaction_manager, buffer_manager,
			table_id2, 11, false));

}

/** 
 * T1 inserts and commits
 * T2 inserts but does not commits
 * T3 inserts but does not commits
 * fuzzy checkpoint begin
 * T2 inserts but does not commits
 * T3 inserts but does not commits
 * T2 commits
 * T3 inserts but does not commits
 * crash
 * Only T1,T2 data should be there
*/
TEST_F(LogManagerTest, TestFuzzyCheckpointCrashDuringCheckpointing){
	BufferManager buffer_manager(128, 10);
	auto logfile = buzzdb::File::open_file(LOG_FILE, buzzdb::File::WRITE);
	LogManager log_manager(logfile.get());
	HeapSegment heap_segment(123, log_manager, buffer_manager);
	TransactionManager transaction_manager(log_manager, buffer_manager);
	
	uint64_t table_id = 101;
	// T1
	do_insert(heap_segment, transaction_manager, buffer_manager, table_id, 1, 2);
	// T2
	uint64_t t2 = transaction_manager.start_txn();
	insert_row(heap_segment, transaction_manager, t2, table_id, 3);
	
	// T3
	uint64_t t3 = transaction_manager.start_txn();
	insert_row(heap_segment, transaction_manager, t3, table_id, 4);
	
	EXPECT_EQ(log_manager.log_fuzzy_checkpoint_begin(buffer_manager), 1);

	insert_row(heap_segment, transaction_manager, t2, table_id, 5);
	insert_row(heap_segment, transaction_manager, t3, table_id, 6);

	transaction_manager.commit_txn(t2);

	insert_row(heap_segment, transaction_manager, t3, table_id, 7);

	crash(transaction_manager, buffer_manager, log_manager);

	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
		table_id, 1, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
		table_id, 2, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 3, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
		table_id, 4, false));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 5, true));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
		table_id, 6, false));
	EXPECT_TRUE(look(heap_segment, transaction_manager, buffer_manager,
			table_id, 7, false));

}

}  // namespace
int main(int argc, char* argv[]) {
	testing::InitGoogleTest(&argc, argv);
//	testing::GTEST_FLAG(filter) = "LogManagerTest.TestOpenCommitCheckpointOpenCrash";
	return RUN_ALL_TESTS();
}
