#ifndef DEFINE_H
#define DEFINE_H



class SqlciEnv;

class Envvar {
  char *name;
  char *value;
  char *env_str;

 public:
  Envvar(const char *name_, const char *value_);
  ~Envvar();

  const char *getName() const { return name; };
  void setName(const char *name_);

  const char *getValue() const { return value; };
  void setValue(const char *value_);

  short contains(const char *value) const;

  Int32 set();
  Int32 reset();
};

#endif
