
/* -*-Java-*-
 ******************************************************************************
 *
 * File:         test.java
 * Description:  Container for validateMethod, the internal SPJ used in 
 *               CREATE PROCEDURE 
 *
 * Created:      October 2002
 * Language:     Java
 * @deprecated   As of SPJ Result Sets feature implementation, moved to 
 *               org.trafodion.sql.udr.LmUtility
 *
 *
 ******************************************************************************
 */

package org.trafodion.sql.udr;

class test
{
  public static void validateMethod(String className,
                                    String methodName,
                                    String externalPath,
                                    String signature,
                                    int numParam,
                                    String optionalSig)
  throws Exception
  {
    LmUtility lmUtil;
    LmClassLoader lmcl;
    Class targetClass;

    //
    // Load the class first using LmClassLoader
    //
    try
    {  
      lmUtil = new LmUtility();

      lmcl = lmUtil.createClassLoader(externalPath, 0);

      lmcl.removeCpURLs();
      targetClass = lmcl.findClass(className);
      lmcl.addCpURLs();
    }
    catch (ClassNotFoundException cnfe)
    {
      throw new MethodValidationFailedException("Class not found: " +
                                                cnfe.getMessage(),
                                                methodName,
                                                signature,
                                                className);

    }
    catch (NoClassDefFoundError cdnfe)
    {
      throw new MethodValidationFailedException("No class definition found: " +
                                                cdnfe.getMessage(),
                                                methodName,
                                                signature,
                                                className);
    }
    catch (Exception e)
    {
      throw new MethodValidationFailedException(e.toString(),
                                                methodName,
                                                signature,
                                                className);
    }

    //
    // Check if the method exists
    //
    lmUtil.verifyMethodSignature(targetClass, methodName, signature, numParam);
  }
}

