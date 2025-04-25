
#include "log/log_manager.h"

#include <string.h>

#include <cassert>
#include <cstddef>
#include <iostream>
#include <set>

#include "common/macros.h"
#include "storage/test_file.h"

namespace buzzdb {

/**
 * Functionality of the buffer manager that might be handy

 Flush all the dirty pages to the disk
        buffer_manager.flush_all_pages():

 Write @data of @length at an @offset the buffer page @page_id
        BufferFrame& frame = buffer_manager.fix_page(page_id, true);
        memcpy(&frame.get_data()[offset], data, length);
        buffer_manager.unfix_page(frame, true);

 * Read and Write from/to the log_file
   log_file_->read_block(offset, size, data);

   Usage:
   uint64_t txn_id;
   log_file_->read_block(offset, sizeof(uint64_t), reinterpret_cast<char *>(&txn_id));
   log_file_->write_block(reinterpret_cast<char *> (&txn_id), offset, sizeof(uint64_t));
 */

LogManager::LogManager(File* log_file) {
    log_file_ = log_file;
    log_record_type_to_count[LogRecordType::ABORT_RECORD] = 0;
    log_record_type_to_count[LogRecordType::COMMIT_RECORD] = 0;
    log_record_type_to_count[LogRecordType::UPDATE_RECORD] = 0;
    log_record_type_to_count[LogRecordType::BEGIN_RECORD] = 0;
    log_record_type_to_count[LogRecordType::CHECKPOINT_RECORD] = 0;
    log_record_type_to_count[LogRecordType::BEGIN_FUZZY_CHECKPOINT_RECORD] = 0;
    log_record_type_to_count[LogRecordType::END_FUZZY_CHECKPOINT_RECORD] = 0;
}

LogManager::~LogManager() {}

void LogManager::reset(File* log_file) {
    log_file_ = log_file;
    current_offset_ = 0;
    txn_id_to_first_log_record.clear();
    log_record_type_to_count.clear();
    fuzzy_checkpoint_page_ids.clear();
}

/// Get log records
uint64_t LogManager::get_total_log_records() {
    return log_record_type_to_count[LogRecordType::ABORT_RECORD]
        + log_record_type_to_count[LogRecordType::COMMIT_RECORD]
        + log_record_type_to_count[LogRecordType::UPDATE_RECORD]
        + log_record_type_to_count[LogRecordType::BEGIN_RECORD]
        + log_record_type_to_count[LogRecordType::CHECKPOINT_RECORD]
        + log_record_type_to_count[LogRecordType::BEGIN_FUZZY_CHECKPOINT_RECORD]
        + log_record_type_to_count[LogRecordType::END_FUZZY_CHECKPOINT_RECORD];
}

uint64_t LogManager::get_total_log_records_of_type(LogRecordType type) {
    return log_record_type_to_count[type];
}

/**
 * Increment the ABORT_RECORD count.
 * Rollback the provided transaction.
 * Add abort log record to the log file.
 * Remove from the active transactions.
 */
void LogManager::log_abort(uint64_t txn_id, BufferManager& buffer_manager) {
    this->log_file_->resize(this->current_offset_ + sizeof(unsigned char) + sizeof(uint64_t));
    const unsigned char TYPE = static_cast<unsigned char>(LogRecordType::ABORT_RECORD);
    this->log_file_->write_block(reinterpret_cast<char*>(&txn_id), current_offset_ + sizeof(unsigned char), sizeof(uint64_t));
    this->log_file_->write_block(reinterpret_cast<const char*>(&TYPE), current_offset_, sizeof(unsigned char));
    this->current_offset_ += sizeof(unsigned char) + sizeof(uint64_t);
    this->log_record_type_to_count[LogRecordType::ABORT_RECORD]++;
    this->rollback_txn(txn_id, buffer_manager);
    this->txn_id_to_first_log_record.erase(txn_id);
}

/**
 * Increment the COMMIT_RECORD count
 * Add commit log record to the log file
 * Remove from the active transactions
 */
void LogManager::log_commit(uint64_t txn_id) {
    this->log_file_->resize(this->current_offset_ + sizeof(unsigned char) + sizeof(uint64_t));
    const unsigned char TYPE = static_cast<unsigned char>(LogRecordType::COMMIT_RECORD);
    this->log_file_->write_block(reinterpret_cast<char*>(&txn_id), current_offset_ + sizeof(unsigned char), sizeof(uint64_t));
    this->log_file_->write_block(reinterpret_cast<const char*>(&TYPE), current_offset_, sizeof(unsigned char));
    this->current_offset_ += sizeof(unsigned char) + sizeof(uint64_t);
    this->log_record_type_to_count[LogRecordType::COMMIT_RECORD]++;
    this->txn_id_to_first_log_record.erase(txn_id);
}

/**
 * Increment the UPDATE_RECORD count
 * Add the update log record to the log file
 * @param txn_id		transaction id
 * @param page_id		buffer page id
 * @param length		length of the update tuple
 * @param offset 		offset to the tuple in the buffer page
 * @param before_img	before image of the buffer page at the given offset
 * @param after_img		after image of the buffer page at the given offset
 */
void LogManager::log_update(uint64_t txn_id, uint64_t page_id, uint64_t length, uint64_t offset, std::byte* before_img, std::byte* after_img) {
    this->log_file_->resize(this->current_offset_ + sizeof(unsigned char) + 4 * sizeof(uint64_t) + 2 * length);
    const unsigned char TYPE = static_cast<unsigned char>(LogRecordType::UPDATE_RECORD);
    this->log_file_->write_block(reinterpret_cast<char*>(&txn_id), current_offset_ + sizeof(unsigned char), sizeof(uint64_t));
    this->log_file_->write_block(reinterpret_cast<char*>(&page_id), current_offset_ + sizeof(unsigned char) + sizeof(uint64_t), sizeof(uint64_t));
    this->log_file_->write_block(reinterpret_cast<char*>(&length), current_offset_ + sizeof(unsigned char) + 2 * sizeof(uint64_t), sizeof(uint64_t));
    this->log_file_->write_block(reinterpret_cast<char*>(&offset), current_offset_ + sizeof(unsigned char) + 3 * sizeof(uint64_t), sizeof(uint64_t));
    this->log_file_->write_block(reinterpret_cast<char*>(before_img), current_offset_ + sizeof(unsigned char) + 4 * sizeof(uint64_t), length);
    this->log_file_->write_block(reinterpret_cast<char*>(after_img), current_offset_ + sizeof(unsigned char) + 4 * sizeof(uint64_t) + length, length);
    this->log_file_->write_block(reinterpret_cast<const char*>(&TYPE), current_offset_, sizeof(unsigned char));
    this->current_offset_ += sizeof(unsigned char) + 4 * sizeof(uint64_t) + 2 * length;
    this->log_record_type_to_count[LogRecordType::UPDATE_RECORD]++;
}

/**
 * Increment the BEGIN_RECORD count
 * Add the begin log record to the log file
 * Add to the active transactions
 */
void LogManager::log_txn_begin(uint64_t txn_id) {
    this->log_file_->resize(this->current_offset_ + sizeof(unsigned char) + sizeof(uint64_t));
    const unsigned char TYPE = static_cast<unsigned char>(LogRecordType::BEGIN_RECORD);
    this->log_file_->write_block(reinterpret_cast<char*>(&txn_id), current_offset_ + sizeof(unsigned char), sizeof(uint64_t));
    this->log_file_->write_block(reinterpret_cast<const char*>(&TYPE), current_offset_, sizeof(unsigned char));
    this->current_offset_ += sizeof(unsigned char) + sizeof(uint64_t);
    uint64_t total_records = this->get_total_log_records();
    this->log_record_type_to_count[LogRecordType::BEGIN_RECORD]++;
    this->txn_id_to_first_log_record.insert({txn_id, total_records});
}

/**
 * Increment the CHECKPOINT_RECORD count
 * Flush all dirty pages to the disk (USE: buffer_manager.flush_all_pages())
 * Add the checkpoint log record to the log file
 */
void LogManager::log_checkpoint(BufferManager& buffer_manager) {
    buffer_manager.flush_all_pages();
    this->log_file_->resize(this->current_offset_ + sizeof(unsigned char));
    const unsigned char TYPE = static_cast<unsigned char>(LogRecordType::CHECKPOINT_RECORD);
    this->log_file_->write_block(reinterpret_cast<const char*>(&TYPE), current_offset_, sizeof(unsigned char));
    this->current_offset_ += sizeof(unsigned char);
    this->log_record_type_to_count[LogRecordType::CHECKPOINT_RECORD]++;
}

/**
 * Increment the BEGIN_FUZZY_CHECKPOINT_RECORD count
 * Determine and store a list of the dirty pages in the buffer pool
 * Return the number of dirty pages
 * Add the fuzzy checkpoint begin log record to the log file
 */
size_t LogManager::log_fuzzy_checkpoint_begin(BufferManager& buffer_manager) {
    this->fuzzy_checkpoint_page_ids = buffer_manager.get_dirty_page_ids();
    this->log_file_->resize(this->current_offset_ + sizeof(unsigned char));
    const unsigned char TYPE = static_cast<unsigned char>(LogRecordType::BEGIN_FUZZY_CHECKPOINT_RECORD);
    this->log_file_->write_block(reinterpret_cast<const char*>(&TYPE), current_offset_, sizeof(unsigned char));
    this->current_offset_ += sizeof(unsigned char);
    this->log_record_type_to_count[LogRecordType::BEGIN_FUZZY_CHECKPOINT_RECORD]++;
    return this->fuzzy_checkpoint_page_ids.size();
}

/**
 * Flush the page at the given step of the fuzzy checkpoint (if it is not already flushed)
 */
void LogManager::log_fuzzy_checkpoint_do_step(BufferManager& buffer_manager, size_t step) {
    if (step >= this->fuzzy_checkpoint_page_ids.size()) {
        return;
    }
    uint64_t page_id = this->fuzzy_checkpoint_page_ids[step];
    buffer_manager.flush_page(page_id);
}

/**
 * Increment the END_FUZZY_CHECKPOINT_RECORD count
 * Add the fuzzy checkpoint end log record to the log file
 */
void LogManager::log_fuzzy_checkpoint_end() {
    this->log_file_->resize(this->current_offset_ + sizeof(unsigned char));
    const unsigned char TYPE = static_cast<unsigned char>(LogRecordType::END_FUZZY_CHECKPOINT_RECORD);
    this->log_file_->write_block(reinterpret_cast<const char*>(&TYPE), current_offset_, sizeof(unsigned char));
    this->current_offset_ += sizeof(unsigned char);
    this->log_record_type_to_count[LogRecordType::END_FUZZY_CHECKPOINT_RECORD]++;
    this->fuzzy_checkpoint_page_ids.clear();
}

class UpdateInfo {
public:
    uint64_t txn_id;
    uint64_t page_id;
    uint64_t length;
    uint64_t offset;
    std::unique_ptr<char[]> before_img;
    std::unique_ptr<char[]> after_img;

    UpdateInfo(uint64_t txn_id, uint64_t page_id, uint64_t length, uint64_t offset, std::unique_ptr<char[]> before_img, std::unique_ptr<char[]> after_img)
        : txn_id(txn_id), page_id(page_id), length(length), offset(offset), before_img(std::move(before_img)), after_img(std::move(after_img)) {}
};

UNUSED_ATTRIBUTE
static void printLog(buzzdb::File *f) {
    size_t size = f->size();
    uint64_t current_offset = 0;
    std::vector<UpdateInfo> updates;
    while (current_offset < size) {
        unsigned char type;
        f->read_block(current_offset, sizeof(unsigned char), reinterpret_cast<char*>(&type));
        if (type == static_cast<unsigned char>(buzzdb::LogManager::LogRecordType::INVALID_RECORD_TYPE)) {
            break;
        }
        if (type == static_cast<unsigned char>(buzzdb::LogManager::LogRecordType::CHECKPOINT_RECORD)) {
            std::cout << "CHECKPOINT" << std::endl;
            current_offset += sizeof(unsigned char);
            continue;
        }
        if (type == static_cast<unsigned char>(buzzdb::LogManager::LogRecordType::BEGIN_FUZZY_CHECKPOINT_RECORD)) {
            std::cout << "BEGIN_FUZZY_CHECKPOINT_RECORD" << std::endl;
            current_offset += sizeof(unsigned char);
            continue;
        }
        if (type == static_cast<unsigned char>(buzzdb::LogManager::LogRecordType::END_FUZZY_CHECKPOINT_RECORD)) {
            std::cout << "END_FUZZY_CHECKPOINT_RECORD" << std::endl;
            current_offset += sizeof(unsigned char);
            continue;
        }
        if (type == static_cast<unsigned char>(buzzdb::LogManager::LogRecordType::BEGIN_RECORD)) {
            uint64_t current_txn_id;
            f->read_block(current_offset + sizeof(unsigned char), sizeof(uint64_t), reinterpret_cast<char*>(&current_txn_id));
            current_offset += sizeof(unsigned char) + sizeof(uint64_t);
            std::cout << "BEGIN " << current_txn_id << std::endl;
            continue;
        }
        if (type == static_cast<unsigned char>(buzzdb::LogManager::LogRecordType::COMMIT_RECORD)) {
            uint64_t current_txn_id;
            f->read_block(current_offset + sizeof(unsigned char), sizeof(uint64_t), reinterpret_cast<char*>(&current_txn_id));
            current_offset += sizeof(unsigned char) + sizeof(uint64_t);
            std::cout << "COMMIT " << current_txn_id << std::endl;
            continue;
        }
        if (type == static_cast<unsigned char>(buzzdb::LogManager::LogRecordType::ABORT_RECORD)) {
            uint64_t current_txn_id;
            f->read_block(current_offset + sizeof(unsigned char), sizeof(uint64_t), reinterpret_cast<char*>(&current_txn_id));
            current_offset += sizeof(unsigned char) + sizeof(uint64_t);
            std::cout << "ABORT " << current_txn_id << std::endl;
        } else {
            uint64_t current_txn_id;
            f->read_block(current_offset + sizeof(unsigned char), sizeof(uint64_t), reinterpret_cast<char*>(&current_txn_id));
            uint64_t length;
            uint64_t page_id;
            uint64_t offset;
            f->read_block(current_offset + sizeof(unsigned char) + sizeof(uint64_t), sizeof(uint64_t), reinterpret_cast<char*>(&page_id));
            f->read_block(current_offset + sizeof(unsigned char) + 2 * sizeof(uint64_t), sizeof(uint64_t), reinterpret_cast<char*>(&length));
            f->read_block(current_offset + sizeof(unsigned char) + 3 * sizeof(uint64_t), sizeof(uint64_t), reinterpret_cast<char*>(&offset));
            current_offset += sizeof(unsigned char) + 4 * sizeof(uint64_t) + 2 * length;
            std::cout << "UPDATE " << current_txn_id << " " << page_id << " " << length << " " << offset << std::endl;
        }
    }
}

/**
 * @Analysis Phase:
 * 		1. Get the active transactions and commited transactions
 * 		2. Restore the txn_id_to_first_log_record
 * @Redo Phase:
 * 		1. Redo the entire log tape to restore the buffer page
 * 		2. For UPDATE logs: write the after_img to the buffer page
 * 		3. For ABORT logs: rollback the transactions
 * 	@Undo Phase
 * 		1. Rollback the transactions which are active and not commited
 */
void LogManager::recovery(BufferManager& buffer_manager) {
    this->log_record_type_to_count[LogRecordType::ABORT_RECORD] = 0;
    this->log_record_type_to_count[LogRecordType::COMMIT_RECORD] = 0;
    this->log_record_type_to_count[LogRecordType::UPDATE_RECORD] = 0;
    this->log_record_type_to_count[LogRecordType::BEGIN_RECORD] = 0;
    this->log_record_type_to_count[LogRecordType::CHECKPOINT_RECORD] = 0;
    this->log_record_type_to_count[LogRecordType::BEGIN_FUZZY_CHECKPOINT_RECORD] = 0;
    this->log_record_type_to_count[LogRecordType::END_FUZZY_CHECKPOINT_RECORD] = 0;
    this->current_offset_ = this->log_file_->size();
    uint64_t current_offset = 0;
    std::vector<UpdateInfo> updatesPending;
    std::vector<UpdateInfo> updatesSinceLastCheckpoint;
    std::set<uint64_t> aborted_txns;

    while (current_offset < this->current_offset_) {
        unsigned char type;
        this->log_file_->read_block(current_offset, sizeof(unsigned char), reinterpret_cast<char*>(&type));
        if (type == static_cast<unsigned char>(LogRecordType::INVALID_RECORD_TYPE)) {
            break;
        }
        if (type == static_cast<unsigned char>(LogRecordType::CHECKPOINT_RECORD)) {
            current_offset += sizeof(unsigned char);
            this->log_record_type_to_count[LogRecordType::CHECKPOINT_RECORD]++;
            updatesPending.clear();
            updatesSinceLastCheckpoint.clear();
            continue;
        }
        if (type == static_cast<unsigned char>(LogRecordType::BEGIN_FUZZY_CHECKPOINT_RECORD)) {
            current_offset += sizeof(unsigned char);
            this->log_record_type_to_count[LogRecordType::BEGIN_FUZZY_CHECKPOINT_RECORD]++;
            updatesPending.clear();
            updatesPending.reserve(updatesSinceLastCheckpoint.size());
            for (auto& update : updatesSinceLastCheckpoint) {
                updatesPending.push_back(std::move(update));
            }
            updatesSinceLastCheckpoint.clear();
            continue;
        }
        if (type == static_cast<unsigned char>(LogRecordType::END_FUZZY_CHECKPOINT_RECORD)) {
            current_offset += sizeof(unsigned char);
            this->log_record_type_to_count[LogRecordType::END_FUZZY_CHECKPOINT_RECORD]++;
            updatesPending.clear();
            continue;
        }
        if (type == static_cast<unsigned char>(LogRecordType::BEGIN_RECORD)) {
            uint64_t txn_id;
            this->log_file_->read_block(current_offset + sizeof(unsigned char), sizeof(uint64_t), reinterpret_cast<char*>(&txn_id));
            uint64_t total_records = this->get_total_log_records();
            this->txn_id_to_first_log_record.insert({txn_id, total_records});
            current_offset += sizeof(unsigned char) + sizeof(uint64_t);
            this->log_record_type_to_count[LogRecordType::BEGIN_RECORD]++;
        } else if (type == static_cast<unsigned char>(LogRecordType::COMMIT_RECORD)) {
            uint64_t txn_id;
            this->log_file_->read_block(current_offset + sizeof(unsigned char), sizeof(uint64_t), reinterpret_cast<char*>(&txn_id));
            txn_id_to_first_log_record.erase(txn_id);
            current_offset += sizeof(unsigned char) + sizeof(uint64_t);
            this->log_record_type_to_count[LogRecordType::COMMIT_RECORD]++;
        } else if (type == static_cast<unsigned char>(LogRecordType::ABORT_RECORD)) {
            uint64_t txn_id;
            this->log_file_->read_block(current_offset + sizeof(unsigned char), sizeof(uint64_t), reinterpret_cast<char*>(&txn_id));
            aborted_txns.insert(txn_id);
            current_offset += sizeof(unsigned char) + sizeof(uint64_t);
            this->log_record_type_to_count[LogRecordType::ABORT_RECORD]++;
        } else {
            uint64_t current_txn_id;
            uint64_t length;
            uint64_t page_id;
            uint64_t offset;
            this->log_file_->read_block(current_offset + sizeof(unsigned char), sizeof(uint64_t), reinterpret_cast<char*>(&current_txn_id));
            this->log_file_->read_block(current_offset + sizeof(unsigned char) + sizeof(uint64_t), sizeof(uint64_t), reinterpret_cast<char*>(&page_id));
            this->log_file_->read_block(current_offset + sizeof(unsigned char) + 2 * sizeof(uint64_t), sizeof(uint64_t), reinterpret_cast<char*>(&length));
            this->log_file_->read_block(current_offset + sizeof(unsigned char) + 3 * sizeof(uint64_t), sizeof(uint64_t), reinterpret_cast<char*>(&offset));
            std::unique_ptr<char[]> before_img = std::make_unique<char[]>(length);
            std::unique_ptr<char[]> after_img = std::make_unique<char[]>(length);
            this->log_file_->read_block(current_offset + sizeof(unsigned char) + 4 * sizeof(uint64_t), length, before_img.get());
            this->log_file_->read_block(current_offset + sizeof(unsigned char) + 4 * sizeof(uint64_t) + length, length, after_img.get());
            updatesSinceLastCheckpoint.push_back(UpdateInfo(current_txn_id, page_id, length, offset, std::move(before_img), std::move(after_img)));
            current_offset += sizeof(unsigned char) + 4 * sizeof(uint64_t) + 2 * length;
            this->log_record_type_to_count[LogRecordType::UPDATE_RECORD]++;
        }
    }

    if (!updatesPending.empty()) {
        for (auto& update : updatesSinceLastCheckpoint) {
            updatesPending.push_back(std::move(update));
        }
        updatesSinceLastCheckpoint.clear();
        updatesSinceLastCheckpoint.reserve(updatesPending.size());
        for (auto& update : updatesPending) {
            updatesSinceLastCheckpoint.push_back(std::move(update));
        }
        updatesPending.clear();
    }

    for (auto& update : updatesSinceLastCheckpoint) {
        if (aborted_txns.find(update.txn_id) == aborted_txns.end()) {
            continue;
        }
        BufferFrame& frame = buffer_manager.fix_page(update.page_id, true);
        memcpy(&frame.get_data()[update.offset], update.after_img.get(), update.length);
        buffer_manager.unfix_page(frame, true);
    }

    for (auto& txn_id : aborted_txns) {
        this->rollback_txn(txn_id, buffer_manager);
    }

    for (auto& txn_id : txn_id_to_first_log_record) {
        if (aborted_txns.find(txn_id.first) == aborted_txns.end()) {
            this->rollback_txn(txn_id.first, buffer_manager);
        }
    }
}

/**
 * Use txn_id_to_first_log_record to get the begin of the current transaction
 * Walk through the log tape and rollback the changes by writing the before
 * image of the tuple on the buffer page.
 * Note: There might be other transactions' log records interleaved, so be careful to
 * only undo the changes corresponding to current transactions.
 */
void LogManager::rollback_txn(uint64_t txn_id, BufferManager& buffer_manager) {
    auto it = this->txn_id_to_first_log_record.find(txn_id);
    if (it == this->txn_id_to_first_log_record.end()) {
        return;
    }
    uint64_t current_offset = 0;
    std::vector<UpdateInfo> updates;
    while (current_offset < this->current_offset_) {
        unsigned char type;
        this->log_file_->read_block(current_offset, sizeof(unsigned char), reinterpret_cast<char*>(&type));
        if (type == static_cast<unsigned char>(LogRecordType::INVALID_RECORD_TYPE)) {
            break;
        }
        if (type == static_cast<unsigned char>(LogRecordType::CHECKPOINT_RECORD)) {
            current_offset += sizeof(unsigned char);
            continue;
        }
        if (type == static_cast<unsigned char>(LogRecordType::BEGIN_FUZZY_CHECKPOINT_RECORD)) {
            current_offset += sizeof(unsigned char);
            continue;
        }
        if (type == static_cast<unsigned char>(LogRecordType::END_FUZZY_CHECKPOINT_RECORD)) {
            current_offset += sizeof(unsigned char);
            continue;
        }
        if (type == static_cast<unsigned char>(LogRecordType::BEGIN_RECORD) || type == static_cast<unsigned char>(LogRecordType::COMMIT_RECORD)) {
            current_offset += sizeof(unsigned char) + sizeof(uint64_t);
            continue;
        }
        if (type == static_cast<unsigned char>(LogRecordType::ABORT_RECORD)) {
            uint64_t current_txn_id;
            this->log_file_->read_block(current_offset + sizeof(unsigned char), sizeof(uint64_t), reinterpret_cast<char*>(&current_txn_id));
            current_offset += sizeof(unsigned char) + sizeof(uint64_t);
            if (current_txn_id == txn_id) {
                break;
            }
        } else {
            uint64_t current_txn_id;
            this->log_file_->read_block(current_offset + sizeof(unsigned char), sizeof(uint64_t), reinterpret_cast<char*>(&current_txn_id));
            uint64_t length;
            if (current_txn_id == txn_id) {
                uint64_t page_id;
                uint64_t offset;
                this->log_file_->read_block(current_offset + sizeof(unsigned char) + sizeof(uint64_t), sizeof(uint64_t), reinterpret_cast<char*>(&page_id));
                this->log_file_->read_block(current_offset + sizeof(unsigned char) + 2 * sizeof(uint64_t), sizeof(uint64_t), reinterpret_cast<char*>(&length));
                this->log_file_->read_block(current_offset + sizeof(unsigned char) + 3 * sizeof(uint64_t), sizeof(uint64_t), reinterpret_cast<char*>(&offset));
                std::unique_ptr<char[]> before_img = std::make_unique<char[]>(length);
                std::unique_ptr<char[]> after_img = std::make_unique<char[]>(length);
                this->log_file_->read_block(current_offset + sizeof(unsigned char) + 4 * sizeof(uint64_t), length, before_img.get());
                this->log_file_->read_block(current_offset + sizeof(unsigned char) + 4 * sizeof(uint64_t) + length, length, after_img.get());
                updates.push_back(UpdateInfo(current_txn_id, page_id, length, offset, std::move(before_img), std::move(after_img)));
            } else {
                this->log_file_->read_block(current_offset + sizeof(unsigned char) + 2 * sizeof(uint64_t), sizeof(uint64_t), reinterpret_cast<char*>(&length));
            }
            current_offset += sizeof(unsigned char) + 4 * sizeof(uint64_t) + 2 * length;
        }
    }

    for (auto it = updates.rbegin(); it != updates.rend(); ++it) {
        BufferFrame& frame = buffer_manager.fix_page(it->page_id, true);
        memcpy(&frame.get_data()[it->offset], it->before_img.get(), it->length);
        buffer_manager.unfix_page(frame, true);
    }
}

}  // namespace buzzdb
