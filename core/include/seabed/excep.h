//------------------------------------------------------------------
//


//
// Exception module
//
#ifndef __SB_EXCEP_H_
#define __SB_EXCEP_H_

#include <exception>

//
// Use these for references.
//
#if __cplusplus < 201103L  // Standards below C++2011 in which
                           // dynamic throw is allowed

#define SB_THROW_FATAL(msg)  throw SB_Fatal_Excep(msg)
#define SB_THROWS_EXCEP(exc) throw(exc)
#define SB_THROWS_FATAL      SB_THROWS_EXCEP(SB_Fatal_Excep)

#else  // Starting C++2011,  use noexcept(bool)
#if defined(BOOST_NO_CXX11_SMART_PTR)
#define SB_THROW_FATAL(msg) throw SB_Fatal_Excep(msg)
#else
#define SB_THROW_FATAL(msg) throw SB_Fatal_Excep(msg)
#endif
#define SB_THROWS_EXCEP(exc) noexcept(false)
#define SB_THROWS_FATAL      noexcept(false)

#endif

//
// Base-class for seabed exceptions.
//
class SB_Excep : public std::exception {
 public:
  SB_Excep(const char *msg);
  SB_Excep(const SB_Excep &excep);  // copy const
  virtual ~SB_Excep() throw();
  SB_Excep &operator=(const SB_Excep &excep);
  // called by terminate
  virtual const char *what() const throw();

 protected:
  char *ip_msg;
};

//
// Fatal seabed exception.
//
class SB_Fatal_Excep : public SB_Excep {
 public:
  SB_Fatal_Excep(const char *msg);
  virtual ~SB_Fatal_Excep() throw();
};

#endif  // !__SB_EXCEP_H_
