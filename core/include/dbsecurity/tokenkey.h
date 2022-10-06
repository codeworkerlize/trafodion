#ifndef _TOKENKEY_H
#define _TOKENKEY_H

#include "dbsecurity/auth.h"
#include <stddef.h>

class TokenKeyContents;

class TokenKey {
  friend class Token;
  friend class TokenContents;

 private:
  static bool IsA(const char *tokenKeyString);

  TokenKey();
  TokenKey(const char *tokenKeyString);
  TokenKey(TokenKey &tokenKey);

  ~TokenKey();

  TokenKey &operator=(const TokenKey &rhs);

  bool operator==(const TokenKey &rhs) const;

  const char *getTokenKey() const;

  void getTokenKeyAsString(char *tokenKeyString) const;

  size_t getTokenKeySize() const;

  void reset();

  bool verify(TokenKey &tokenKey) const;

  bool verifyParent(char *blackBox, size_t &blackboxLength, AuthFunction authFn) const;

  TokenKeyContents &self;
};

#endif /* _TOKENKEY_H */
