#include "src/include/pfm.h"

#include <cstdio>
#include <unistd.h>
#include <memory>

namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }
    PagedFileManager::PagedFileManager() = default;
    PagedFileManager::~PagedFileManager() = default;
    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;
    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {
        const char* fileName_c = fileName.c_str();
        //if fileName does not already exist
        if (access(fileName_c, F_OK) != 0) {
            FILE * pageFile = fopen(fileName_c, "wb+");
            if (!pageFile) {
                return -1; // Failed to create file
            }
            //create a hidden page in the file here
            FileHandle fileHandle;
            fileHandle.openedFile = pageFile;
            fileHandle.createHiddenPage();
            fclose(pageFile);
            return 0;
        }
        // file already exists
        return -1;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        const char* fileName_c = fileName.c_str();
        if (access(fileName_c, F_OK) == 0) {
            if (remove(fileName_c) == 0) {return 0;}
        }
        // file does not exist
        return -1;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        const char* fileName_c = fileName.c_str();
        if (access(fileName_c, F_OK) == 0) {
            FILE* pageFile = fopen(fileName_c, "rb+");
            if (pageFile == nullptr) {
                return -1;
            }
            fileHandle.updateOpenedFile(pageFile);
            return 0;
        }
        // file does not exist
        return -1;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        if (fileHandle.openedFile != nullptr) {
            fclose(fileHandle.openedFile);
            fileHandle.openedFile = nullptr;
            return 0;
        }
        // the file is not opened
        return -1;

    }

    FileHandle::FileHandle(): openedFile(nullptr), readPageCounter(0), writePageCounter(0), appendPageCounter(0) {}
    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if (pageNum < getNumberOfPages()) {
            // Read pageNum + 1 to skip the hidden page
            fseek(openedFile, (pageNum + 1) * PAGE_SIZE, SEEK_SET);
            size_t result = fread(data, sizeof(char), PAGE_SIZE, openedFile);
            if (result == PAGE_SIZE) {
                //update readPageCounter
                readPageCounter++;
                setReadPageCnt(readPageCounter);
                return 0;
            }
        }
        // page does not exist
        return -1;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        PageNum totalPages = getNumberOfPages();
        if (pageNum >= totalPages) {
            return -1;
        }
        fseek(openedFile, (pageNum + 1) * PAGE_SIZE, SEEK_SET);
        size_t result = fwrite(data, sizeof(char), PAGE_SIZE, openedFile);
        fflush(openedFile);
        if (result == PAGE_SIZE) {
            //update writePageCounter
            writePageCounter++;
            setWritePageCnt(writePageCounter);
            return 0;
        }

        // page does not exist
        return -1;
    }

    RC FileHandle::appendPage(const void *data) {
        fseek(openedFile, 0, SEEK_END); // write to end of file
        fwrite(data, sizeof(char), PAGE_SIZE, openedFile);
        fflush(openedFile);
        //update appendPageCounter
        appendPageCounter++;
        setAppendPageCnt(appendPageCounter);
        //update the number of pages
        setNumberOfPages(getNumberOfPages() + 1);
        return 0;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
        return 0;
    }

    void FileHandle::updateOpenedFile(FILE * pageFile) {
        openedFile = pageFile;
        readPageCounter = getCounterValue(READ_PAGE_CNT_POS);
        writePageCounter = getCounterValue(WRITE_PAGE_CNT_POS);
        appendPageCounter = getCounterValue(APPEND_PAGE_CNT_POS);
    }

    unsigned FileHandle::getCounterValue(int offset) {
        unsigned counter = 0;
        fseek(openedFile, offset * sizeof(unsigned), SEEK_SET);
        fread(&counter, sizeof(unsigned), 1, openedFile);
        return counter;
    }

    RC FileHandle::setCounterValue(unsigned value, int offset) {
        fseek(openedFile, offset * sizeof(unsigned), SEEK_SET);
        fwrite(&value, sizeof(unsigned), 1, openedFile);
        fflush(openedFile);
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        return getCounterValue(NUM_PAGE_POS);
    }

    RC FileHandle::setNumberOfPages(unsigned numOfPages) {
        return setCounterValue(numOfPages, NUM_PAGE_POS);
    }

    RC FileHandle::setReadPageCnt(unsigned readPageCnt) {
        return setCounterValue(readPageCnt, READ_PAGE_CNT_POS);
    }

    RC FileHandle::setWritePageCnt(unsigned writePageCnt) {
        return setCounterValue(writePageCnt, WRITE_PAGE_CNT_POS);
    }

    RC FileHandle::setAppendPageCnt(unsigned appendPageCnt) {
        return setCounterValue(appendPageCnt, APPEND_PAGE_CNT_POS);
    }

    void FileHandle::createHiddenPage() {
        // creating the hidden page does not increment page count
        std::unique_ptr<char[]> pageInfo(new char[PAGE_SIZE]);

        // Initialize numPage, readPage, writePage, and appendPage count
        std::fill(pageInfo.get(), pageInfo.get() + PAGE_SIZE, 0);

        //write the hidden page
        fwrite(pageInfo.get(), sizeof(char), PAGE_SIZE, openedFile);
        fflush(openedFile);
    }

} // namespace PeterDB