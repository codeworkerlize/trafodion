#ifndef NAMEFILTER_H
#define NAMEFILTER_H
#include <ctype.h>
#include <list>
#include <map>
#include <unordered_set>
#include <unordered_map>

enum NameType { USERNAME = 1, GROUPNAME, OTHER };

class NameFilter {
 public:
  const char *getDatabaseName(char *, NameType);

  void addNamesToFilter(char *, size_t, NameType);

  // filter both userNames/groupNames and nameFilter
  void filterNames(std::map<size_t, char *> *, std::map<size_t, char *> *, std::map<size_t, char *> *, NameType);

  // filter only userNames/groupNames
  void filterNames(std::map<size_t, char *> *, std::map<size_t, char *> *, NameType);

  ~NameFilter();

  std::list<char *> *getGroupNames();

  static std::map<size_t, char *> *covertToHashMap(std::list<char *> *);

  static size_t getNameHash(const char *);

 private:
  std::unordered_set<size_t> nameFilter_;
  std::unordered_map<size_t, char *> userNames;
  std::unordered_map<size_t, char *> groupNames;
};
#endif