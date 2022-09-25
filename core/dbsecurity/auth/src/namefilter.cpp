#include "inc/namefilter.h"
#include <string.h>
#include <tr1/functional>
#include <iostream>
#include <fstream>

using namespace std;
using namespace tr1;

void NameFilter::addNamesToFilter(char *name, size_t hash, NameType type)
{
    std::unordered_map<size_t, char *> *pMap = NULL;
    if (NULL == name)
        return;
    this->nameFilter_.insert(_Fnv_hash::hash(name, strlen(name)));
    if (NameType::OTHER == type)
        return;
    pMap = (NameType::GROUPNAME == type) ? &(this->groupNames) : &(this->userNames);
    pMap->insert(make_pair(hash, name));
}

void NameFilter::filterNames(map<size_t, char *> *objects, map<size_t, char *> *needRegisterObjects,
                             map<size_t, char *> *registeredObjects, NameType type)
{
    unordered_map<size_t, char *> *pMap = (NameType::GROUPNAME == type) ? &(this->groupNames) : &(this->userNames);

    if (pMap->empty())
    {
        for (map<size_t, char *>::iterator itor = objects->begin(); itor != objects->end(); itor++)
        {
            size_t hash = itor->first;
            char *name = itor->second;
            map<size_t, char *> *pDstMap = (0 != this->nameFilter_.count(hash)) ? registeredObjects : needRegisterObjects;
            pDstMap->insert(make_pair(hash, name));
        }
        return;
    }

    for (map<size_t, char *>::iterator itor = objects->begin(); itor != objects->end(); itor++)
    {
        size_t hash = itor->first;
        char *name = itor->second;
        if (0 == pMap->count(hash))
        {
            map<size_t, char *> *pDstMap = (0 != this->nameFilter_.count(hash)) ? registeredObjects : needRegisterObjects;
            pDstMap->insert(make_pair(hash, name));
        }
    }
}

void NameFilter::filterNames(map<size_t, char *> *objects, map<size_t, char *> *nonRegisterObjects, NameType type)
{
    if (NameType::OTHER == type)
        return;
    unordered_map<size_t, char *> *pMap = (NameType::GROUPNAME == type) ? &(this->groupNames) : &(this->userNames);

    if (pMap->empty())
    {
        for (map<size_t, char *>::iterator itor = objects->begin(); itor != objects->end(); itor++)
            nonRegisterObjects->insert(make_pair(itor->first, itor->second));
        return;
    }

    for (map<size_t, char *>::iterator itor = objects->begin(); itor != objects->end(); itor++)
    {
        size_t hash = itor->first;
        char *name = itor->second;
        if (0 == pMap->count(hash))
            nonRegisterObjects->insert(make_pair(hash, name));
    }
}

map<size_t, char *> *NameFilter::covertToHashMap(list<char *> *names)
{
    map<size_t, char *> *retMap = new map<size_t, char *>();
    for (list<char *>::iterator itor = names->begin(); itor != names->end(); itor++)
    {
        size_t hash = _Fnv_hash::hash(*itor, strlen(*itor));
        retMap->insert(make_pair(hash, *itor));
    }
    return retMap;
}

const char *NameFilter::getDatabaseName(char *name, NameType type)
{
    if (NameType::OTHER == type)
        return NULL;
    unordered_map<size_t, char *> *pMap = (NameType::USERNAME == type) ? &(this->userNames) : &(this->groupNames);
    size_t hash = NameFilter::getNameHash(name);
    unordered_map<size_t, char *>::iterator itor = pMap->find(hash);
    return (itor != pMap->end()) ? (const char *)(itor->second) : NULL;
}

list<char *> *NameFilter::getGroupNames()
{
    list<char *> *groups = new list<char *>();
    for (unordered_map<size_t, char *>::iterator itor = this->groupNames.begin(); itor != this->groupNames.end(); itor++)
        groups->push_back(itor->second);
    return groups;
}

size_t NameFilter::getNameHash(const char *name)
{
    return _Fnv_hash::hash(name, strlen(name));
}

NameFilter::~NameFilter()
{
    for (unordered_map<size_t, char *>::iterator itor = this->groupNames.begin(); itor != this->groupNames.end(); itor++)
        delete[] itor->second;
    for (unordered_map<size_t, char *>::iterator itor = this->userNames.begin(); itor != this->userNames.end(); itor++)
        delete[] itor->second;
}