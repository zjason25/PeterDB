#include "src/include/pfm.h"

#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <cstring>

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

    FileHandle::FileHandle() {
        openedFile = nullptr; // FILE * . default a nullptr
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        PageNum totalPages = getNumberOfPages();
        if (pageNum <= totalPages) {
            PageNum page = pageNum * PAGE_SIZE + PAGE_SIZE; // + PAGE_SIZE to skip first page
            fseek(openedFile, (long)(page * sizeof(char)), SEEK_SET);
            fread(data, sizeof(char), PAGE_SIZE, openedFile);
            //update readPageCounter
            readPageCounter = getReadPageCnt() + 1;
            setReadPageCnt(readPageCounter);
            return 0;
        }
        // page does not exist
        return -1;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        PageNum totalPages = getNumberOfPages();
        if (totalPages == 0 && pageNum == 0) {appendPage(data);}
        else if (pageNum <= totalPages) {
            PageNum page = pageNum * PAGE_SIZE + PAGE_SIZE; // + PAGE_SIZE to skip first page
            fseek(openedFile,  (long)(page * sizeof(char)), SEEK_SET);
            fwrite(data, sizeof(char), PAGE_SIZE, openedFile);
            fflush(openedFile);
            //update writePageCounter
            writePageCounter = getWritePageCnt() + 1;
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
        appendPageCounter = getAppendPageCnt() + 1;
        setAppendPageCnt(appendPageCounter);
        //update the number of pages
        unsigned pageNumber = getNumberOfPages() + 1;
        setNumberOfPages(pageNumber);
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
        readPageCounter = getReadPageCnt();
        writePageCounter = getWritePageCnt();
        appendPageCounter = getAppendPageCnt();
    }

    unsigned FileHandle::getNumberOfPages() {
        unsigned numPages = 0;
        fseek(openedFile, 0, SEEK_SET); // the start
        fread(&numPages, sizeof(unsigned), 1, openedFile);
        return numPages;
    }

    unsigned FileHandle::getReadPageCnt() {
        unsigned readPageCnt = 0;
        fseek(openedFile, sizeof(unsigned), SEEK_SET); // 1 offset
        fread(&readPageCnt, sizeof(unsigned), 1, openedFile);
        return readPageCnt;
    }

    unsigned FileHandle::getWritePageCnt() {
        unsigned writePageCnt = 0;
        fseek(openedFile, sizeof(unsigned)*2, SEEK_SET); // 2 offsets over
        fread(&writePageCnt, sizeof(unsigned), 1, openedFile);
        return writePageCnt;
    }

    unsigned FileHandle::getAppendPageCnt() {
        unsigned appendPageCnt = 0;
        fseek(openedFile, sizeof(unsigned)*3, SEEK_SET); // 3 offsets over
        fread(&appendPageCnt, sizeof(unsigned), 1, openedFile);
        return appendPageCnt;
    }

    RC FileHandle::setNumberOfPages(unsigned numOfPages) {
        fseek(openedFile, 0, SEEK_SET); // the start
        fwrite(&numOfPages, sizeof(unsigned), 1, openedFile);
        fflush(openedFile);
        return 0;
    }

    RC FileHandle::setReadPageCnt(unsigned readPageCnt) {
        fseek(openedFile, sizeof(unsigned), SEEK_SET); // 1 offset over
        fwrite(&readPageCnt, sizeof(unsigned), 1, openedFile);
        fflush(openedFile);
        return 0;
    }

    RC FileHandle::setWritePageCnt(unsigned writePageCnt) {
        fseek(openedFile, sizeof(unsigned)*2, SEEK_SET); // 2 offsets over
        fwrite(&writePageCnt, sizeof(unsigned), 1, openedFile);
        fflush(openedFile);
        return 0;
    }

    RC FileHandle::setAppendPageCnt(unsigned appendPageCnt) {
        fseek(openedFile, sizeof(unsigned)*3, SEEK_SET); // 3 offsets over
        fwrite(&appendPageCnt, sizeof(unsigned), 1, openedFile);
        fflush(openedFile);
        return 0;
    }

    void FileHandle::createHiddenPage() {
        void *pageInfo = malloc(PAGE_SIZE);
        unsigned numPages = 0, readPageCnt = 0, writePageCnt = 0, appendPageCnt = 0;
        unsigned storage_size = sizeof (unsigned);

        char* bytePtr = static_cast<char*>(pageInfo);
        memcpy(bytePtr,&numPages,storage_size);
        bytePtr+storage_size;
        memcpy(bytePtr,&readPageCnt,storage_size);
        bytePtr+storage_size;
        memcpy(bytePtr,&writePageCnt,storage_size);
        bytePtr+storage_size;
        memcpy(bytePtr,&appendPageCnt,storage_size);

        //write the hidden page
        fwrite(pageInfo, sizeof(char), PAGE_SIZE, openedFile);
        fflush(openedFile);
        free(pageInfo);
    }

} // namespace PeterDB