#ifndef _TOKEN_H
#define _TOKEN_H

#include <stddef.h>

#include "dbsecurity/auth.h"

class TokenContents;
class TokenKey;

class Token {
 public:
  static Token *Obtain();

  static Token *Verify(const char *tokenKey);

  static Token *Verify(const char *tokenKey, AuthFunction authFn);

  void getData(char *data) const;

  size_t getDataSize() const;

  void getTokenKey(char *tokenKey) const;

  void getTokenKeyAsString(char *tokenKeyString) const;

  size_t getTokenKeySize() const;

  void reset();

  void setData(const char *data, size_t length);

 private:
  TokenContents &self;

  Token();
  Token(TokenKey &tokenKey);

  ~Token();
  Token(Token &);
};

#endif /* _TOKEN_H */
