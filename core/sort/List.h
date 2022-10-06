
#ifndef LIST_H_
#define LIST_H_
#include "SortError.h"
#include "common/Platform.h"
#include "export/NABasicObject.h"

template <class T>
class ListNode : public NABasicObject {
 public:
  ListNode(T thing);
  void deleteNode();
  ListNode *next;
  T item;
};

template <class T>
class List : public NABasicObject {
 public:
  List();
  ~List();

  void append(T item, CollHeap *heap);
  void prepend(T item, CollHeap *heap);
  void deleteList();
  T first();

 private:
  ListNode<T> *head;
  ListNode<T> *tail;
  int numItems;
};

#include "List.cpp"
#endif
