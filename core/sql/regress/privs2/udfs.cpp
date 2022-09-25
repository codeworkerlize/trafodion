// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sqludr.h"

extern "C" {

/* genPhoneNumber */
SQLUDR_LIBFUNC SQLUDR_INT32 genPhoneNumber(SQLUDR_INT32 *in1, // seed
                                           SQLUDR_CHAR *in2, // areacode 
                                           SQLUDR_CHAR *out,
                                           SQLUDR_INT16 *in1Ind,
                                           SQLUDR_INT16 *in2Ind,
                                           SQLUDR_INT16 *outInd,
                                           SQLUDR_TRAIL_ARGS)
{
  if (calltype == SQLUDR_CALLTYPE_FINAL)
    return SQLUDR_SUCCESS;

  std::string result (in2);
  if (*in1Ind == SQLUDR_NULL || *in2Ind == SQLUDR_NULL)
  {
    *outInd = SQLUDR_NULL;
  }
  else
  {
    srand(*in1);
    int number = 7;  // 7 digit phone number
    for (int i = 0; i < number; i++)
    {
      int randNumber = rand() %10;
      if (i == 0 && randNumber == 0)
        randNumber++;
      switch (randNumber)
      {
         case 0: result += '0'; break;
         case 1: result += '1'; break;
         case 2: result += '2'; break;
         case 3: result += '3'; break;
         case 4: result += '4'; break;
         case 5: result += '5'; break;
         case 6: result += '6'; break;
         case 7: result += '7'; break;
         case 8: result += '8'; break;
         default : result += '9'; break;
      }
    }
  }

  strcpy(out, result.c_str());
  return SQLUDR_SUCCESS;
}


/* genRandomNumber */
SQLUDR_LIBFUNC SQLUDR_INT32 genRandomNumber(SQLUDR_INT32 *in1,
                                            SQLUDR_INT32 *in2,
                                            SQLUDR_CHAR *out,
                                            SQLUDR_INT16 *in1Ind,
                                            SQLUDR_INT16 *in2Ind,
                                            SQLUDR_INT16 *outInd,
                                            SQLUDR_TRAIL_ARGS)
{
  if (calltype == SQLUDR_CALLTYPE_FINAL)
    return SQLUDR_SUCCESS;

  std::string result;
  if (*in1Ind == SQLUDR_NULL || *in2Ind == SQLUDR_NULL)
  {
    *outInd = SQLUDR_NULL;
  }
  else
  {
    int number = *in2;
    for (int i = 0; i < number; i++)
    {
      int randNumber = rand() %10;
      if (i == 0 && randNumber == 0)
        randNumber++;
      switch (randNumber)
      {
         case 0: result += '0'; break;
         case 1: result += '1'; break;
         case 2: result += '2'; break;
         case 3: result += '3'; break;
         case 4: result += '4'; break;
         case 5: result += '5'; break;
         case 6: result += '6'; break;
         case 7: result += '7'; break;
         case 8: result += '8'; break;
         default : result += '9'; break;
      }
    }
  }

  strcpy(out, result.c_str());
  return SQLUDR_SUCCESS;
}

SQLUDR_LIBFUNC SQLUDR_INT32 genTimestamp(SQLUDR_INT64 *in1,
                                         SQLUDR_INT32 *in2,
                                         SQLUDR_INT64 *in3,
                                         SQLUDR_INT64 *out,
                                         SQLUDR_INT16 *in1Ind,
                                         SQLUDR_INT16 *in2Ind,
                                         SQLUDR_INT16 *in3Ind,
                                         SQLUDR_INT16 *outInd,
                                         SQLUDR_TRAIL_ARGS)
{
  if (calltype == SQLUDR_CALLTYPE_FINAL)
    return SQLUDR_SUCCESS;

  // in1 - seed
  // in2 - number days ahead to generate time
  // in3 - julian time of start day

  // 86400000000 is one day
  long day = 86400000000;
  long hour = day/24;
  long min = hour/60;
  long seconds = min/60;

  long starttime = *in3;
  int numberDays = *in2;
  long low = starttime;
  long high = starttime * numberDays;
  long randNumber = low;

  if (*in1Ind == SQLUDR_NULL ||
      *in2Ind == SQLUDR_NULL ||
      *in3Ind == SQLUDR_NULL)
  {
    *outInd = SQLUDR_NULL;
  }
  else
  {
     // Generate a random timestamp between the above values
     srand(*in1);
     int randDay = rand() %(60 - 1) + 1; // day increment
     int randHour = rand() %(24); // hour increment
     int randMin = rand() %(60);  // min increment
     int randSec = rand() %(60 - 1) + 1; // seconds increment
     randNumber = low + (day * randDay) + (hour * randHour) +
                  (min * randMin) + (seconds * randSec) ;
  }

  *out = randNumber;
  return SQLUDR_SUCCESS;
}

} /* extern "C" */

