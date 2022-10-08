
#ifndef LIST_H_
#define LIST_H_
#include "common/Platform.h"
#include "export/NABasicObject.h"
#include "sort/SortError.h"

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

#include "executor/ex_ex.h"
#include "sort/Const.h"

#if !defined(FORDEBUG) && !defined(NDEBUG)
#define NDEBUG 1
#endif

template <class T>
ListNode<T>::ListNode(T thing) : next(NULL), item(thing) {}

template <class T>
void ListNode<T>::deleteNode() {
  delete item;
}

template <class T>
List<T>::List() : head(NULL), tail(NULL), numItems(0) {}

template <class T>
List<T>::~List() {
  ListNode<T> *temp;
  for (int i = 0; i < numItems; i++) {
    temp = head;
    head = temp->next;
    delete temp;
  }
  tail = NULL;
}

template <class T>
void List<T>::append(T item, CollHeap *heap) {
  ListNode<T> *temp = new (heap) ListNode<T>(item);
  if (numItems == 0) {
    head = temp;
  } else {
    tail->next = temp;
  }
  tail = temp;
  numItems++;
}

template <class T>
void List<T>::prepend(T item, CollHeap *heap) {
  ListNode<T> *temp = new (heap) ListNode<T>(item);
  if (numItems == 0) {
    tail = temp;
  } else {
    temp->next = head;
  }
  head = temp;
  numItems++;
}

template <class T>
void List<T>::deleteList() {
  ex_assert(numItems > 0, "List<T>::deleteList(), numItems <=0");
  ListNode<T> *temp;
  while (head != NULL) {
    temp = head;
    head = temp->next;
    temp->deleteNode();
    delete temp;
    numItems--;
  }
  tail = NULL;
}

template <class T>
T List<T>::first() {
  ex_assert(numItems > 0, "T List<T>::first(), numItems<=0");

  ListNode<T> *temp = head;

  if (numItems == 1) {
    head = tail = NULL;
  } else {
    head = temp->next;
  }
  numItems--;
  T temp2 = temp->item;
  delete temp;
  return temp2;
}

#endif
