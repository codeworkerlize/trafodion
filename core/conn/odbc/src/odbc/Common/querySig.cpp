/**********************************************************************
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
********************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <algorithm>
#include <string.h>

#include <boost/crc.hpp> 
using namespace std;
using namespace boost;

int consumeSpaces(string s, int index);

string GetClusterID(string input);  
extern "C" {
char *querySignature(char*in)
{
  char *retstr;
  string output,input;
  input = in;
  output=GetClusterID(input);  
  retstr = (char*)malloc(output.size()+1);
  memset(retstr,0,output.size()+1);
  strcpy(retstr,output.c_str());
  return retstr;
}
}

long toCRC32(string str){
    //Convert string to bytes
    
    return str.size();
}
void logError(string s, string error, int pos){
  //cerr << s << ": Parsing error:" << error <<" at position: " <<pos << endl;
  return;
}

int isXDigit(char c)
{
  if(c>='0' && c<='9') return 1;
  if(c>'A' && c <='F' ) return 1;
  return -1;
}

int isDDigit(char c)
{
  if(c>='0' && c<='9') return 1;
  return -1;
}

bool isValidIdentifierChar(char c)
{
	return  (c>='A' && c<='Z')|| isDDigit(c) == 1 || c=='_' || c=='@' || c =='#' || c == '$' || c > 128 ;
}

	
// perform the equivalent of replaceAll("IN\\s*\\((?!\\s*SELECT).*a.*(,.*a.*)*\\)", "IN(a)"). Did it this way because the regex way add severe performance drawback
string SilenceINStatements(string input){
	int inputlength = input.size();
	string sb = "";
	
	for (int i=0; i<input.size(); i++){
		
		int index = i;
		char c = input.at(index);
		sb+=c;
		if (c != ' ' && c != '\t' && c != '\r' && c != '\n'){
			continue;
		}
		index++;
		if (index == inputlength || (c = input.at(index)) != 'I'){
			continue;			
		}
		index++;
		if (index == inputlength || (c = input.at(index)) != 'N'){
			continue;			
		}
		index++;
		index = consumeSpaces(input, index);
		if (index == inputlength || (c = input.at(index)) != '(')
			continue;
		index++;
		if (index == inputlength)
			continue;
		index = consumeSpaces(input, index);
		if (index == inputlength)
			continue;
		if (index+5 > inputlength || ( input.at(index) =='S'
				&& input.at(index+1) =='E'
				&& input.at(index+2) =='L'
				&& input.at(index+3) =='E'
				&& input.at(index+4) =='C'
				&& input.at(index+5) =='T')
				)
			continue;
		int parenthes = 0; // parentheses counter
		bool detected = false;
		while(index<inputlength){
			while(index<inputlength && (c=input.at(index)) != 'a'){
				if (c=='(')
					parenthes++;
				else
					if (c==')')
				parenthes--;
				index++;
			}
			if (index== inputlength)
				break;
			if (parenthes < 0)
				break;
			index++;//consume 'a'
			while(index<inputlength && (c=input.at(index)) != ',' && !(c == ')'  && parenthes==0)){
				if (c=='(')
					parenthes++;
				else
					if (c==')')
						parenthes--;
				index++;
			}
			if (c==')' && parenthes==0)
			{
				i=index;
				sb+="IN(a)"; // detected.
				detected=true;
				break;
			}
		}
		//if reaching here, the IN(a,a... was truncated
		if(!detected){
			i=index;
 			sb+="IN(a)";
		}
	}
		
	return sb;
	
}

// return next index that is not space equivalent
int consumeSpaces(string s, int index){
	int result = index;
	char *ptr = (char *) malloc(s.size() + 1);
	strcpy(ptr, s.c_str() );
	
	while (result<s.size()){
		char c = ptr[result];
		if (c == ' ' || c == '\t' || c == '\r' || c == '\n')
			result++;
		else
			break;
	}
	free(ptr);
	return result;
}

// define start of non delimited identifier or keywords
bool isValidIdentifierStartChar(char c)
{		
	return  (c>='A' && c<='Z') || c=='_' || c=='@' || c =='#' || c == '$' || c>128 ;
}
	
bool parseTRUE(string input, int pos){
	char * inputArray = (char *) malloc(input.size() + 1);
	strcpy(inputArray,input.c_str());
	bool ret = true;
	
	if (pos+3>=input.size()){free(inputArray); return false;}
		if (inputArray[pos] =='T' &&
			inputArray[pos+1] =='R' &&
			inputArray[pos+2] =='U' &&
			inputArray[pos+3] =='E' &&
			(pos+4==input.size() || !isValidIdentifierChar(inputArray[pos+4])))
				ret = true;
			else
				ret = false;
	free(inputArray);
	return ret;
}

bool parseUNKNOWN(string input, int pos){
	if (pos+6>=input.size()) return false;
	bool ret = true;
	
	char *inputArray = (char *) malloc(input.size() + 1);
	strcpy(inputArray, input.c_str());
	
	if (inputArray[pos] =='U' &&
		inputArray[pos+1] =='N' &&
		inputArray[pos+2] =='K' &&
		inputArray[pos+3] =='N' &&
		inputArray[pos+4] =='O' &&
		inputArray[pos+5] =='W' &&
		inputArray[pos+6] =='N' &&
		(pos+7== input.size() || !isValidIdentifierChar(inputArray[pos+7])))
			ret = true;
		else
			ret= false;
			
		free(inputArray);
		return ret;
}

char getPrecedingNonWhiteSpaceOrTabChar(string input,int pos){
	int p = pos - 1;
	char ret;
	char *inputArray;
	inputArray = (char *)malloc(input.size() + 1);
	while (p>=0 && (inputArray[p]==' ' ||inputArray[p]=='\t')){
		p--;
	}
	if (p<0){ free(inputArray); return ' ';}
	ret = inputArray[p];
	free(inputArray);
	return ret;
}
	
// return -1 if failed to parse a numeric, else return the new position
int parseNumeric(string input,int pos){
	int p = -1;
	char *inputArray = (char*)malloc(input.size()+1);
	strcpy(inputArray,input.c_str());
	int length = input.size();
	char c = inputArray[pos];
	
	if  (c == '0' && pos+1<length && inputArray[pos+1] =='X'){//parse hexadecimal
		p = pos+2;
		while (p<length && isXDigit(inputArray[p])>=0){ //
			p++;
		}
		if (p == pos+2) p = -1; // make sure at least on hexadecimal digit follow 0X
	}else{
		p = pos+1;
		while (p<length && isDDigit(inputArray[p])==1){ // parse first integer
			p++;
		}
		if (p<length && inputArray[p]=='.'){// parse decimal
			p++;
			while (p<length && isDDigit(inputArray[p])==1){ // parse second integer after .
				p++;
			}				
		}
		if (p<length && inputArray[p] == 'E'){ // check for Exponent
			p++;
			if (p<length && (inputArray[p] == '-' || inputArray[p] == '+' )) // skip sign if needed
				p++;
			if (p<length && isDDigit(inputArray[p])==1) // make sure we have at least one digit after the exponent
				p++;
			else
				{free(inputArray);return -1;}
			while (p<length && isDDigit(inputArray[p])==1 ){ // consume any extra digit
				p++;
			}				
		}
	}
	free(inputArray);
	return p;
}	
bool parseNULL(string inputArray, int pos) {
	if (pos+3 >= inputArray.size() ) return false;
	string nextFourChars = inputArray.substr(pos,4);
	if(nextFourChars == "NULL" && (pos+4 == inputArray.size() || !isValidIdentifierChar(inputArray[pos+4])) )
		return true;
	else
		return false;
	}
bool parseFALSE(string inputArray, int pos){
	if (pos+4>=inputArray.size()) return false;
	string nextFiveChars=inputArray.substr(pos,5);
	if(nextFiveChars == "FALSE" && (pos+5 == inputArray.size() || !isValidIdentifierChar(inputArray[pos+5])) )
		return true;
	else 
		return false;
}
// return the uniqueID identifying the class of the query passed as input parameter
// The algorithm used is the following:
// 1-Upper case the input string 
// 2-parse character by character and
//		- remove any single line comment
//		- remove any multiple line comment
//		- detect and replace any signed or unsigned numerics, including hexadecimal, decimal, approximate number form by 'a'
//		- replace BOOLEAN literals by 'a'
//		- replace any '*' with 'a'. This silences all literals that uses single quotes.
// 3- detect and replace any IN(literal, literals...) statements with IN() using regular expression
// 4- CRC32 Hash the resulting string and return it as result (MD5 is mode computation for no reason)

string GetClusterID(string sqlString)
{
	//convert input string into upper-case
	string inputArray = sqlString;
	std::transform(inputArray.begin(), inputArray.end(),inputArray.begin(), ::toupper);

	int pos = 0; // current position in inputArray
	string sb = ""; // roughly expect size of the filtered output to be same as input
	string REPLACED="a";
	
	while(pos<inputArray.size()){
		char ch = inputArray.at(pos);
		if (parseNULL(inputArray, pos)) {
			sb += REPLACED;
			pos = pos + 3;
		}
		else if (parseFALSE(inputArray, pos)){ //is it FALSE literal
			sb+=REPLACED; // replace boolean literal with REPLACE token
			pos = pos + 4; // size of FALSE - 1
		}else if (parseTRUE(inputArray, pos)){ //is it TRUE literal
			sb+=REPLACED; // replace boolean literal with REPLACE token
			pos = pos + 3; // size of TRUE - 1
		}else if (parseUNKNOWN(inputArray, pos)){ //is it UNKNOWN literal
			sb+=REPLACED; // replace boolean literal with REPLACE token
			pos = pos + 6; // size of UNKNOWN - 1
		}else if (isValidIdentifierStartChar(ch)){ // is it valid IDENTIFIER OR KEYWORD
			sb+=ch;
			pos++;
			while(pos<inputArray.size() && isValidIdentifierChar(inputArray.at(pos))){
				sb+=inputArray.at(pos);
				pos++;
			}
			pos--;
		}else if (isDDigit(ch)==1){ // NUMERIC LITERAL CASE?					
			// parse all possible numbers to advance the position
			int jump = parseNumeric(inputArray,pos);
			if (jump == -1){
				logError(sqlString,"Parse Numeric", pos);
			}
			else
			{
				pos = jump - 1; // -1 to accommodate that we do a pos++ at the end
				sb+=REPLACED; // replace Numeric literal with REPLACE token
			}
		}else if (ch == '\''){ // SINGLE QUOTE LITERAL CASE?
			//move cursor to closing single quote
			pos++;
			try{
				while(inputArray.at(pos) != '\'' || ((pos+1<inputArray.size()) && inputArray.at(pos+1) == '\'')){ //bypass the double single quote escape char
					if (inputArray.at(pos) == '\'')
						pos++;// skip one more if we are in escape sequence
						pos++;
					}
				sb+=REPLACED; // replace single quoted string based literal with REPLACE token
			} catch(...){
				logError(sqlString,"end of line truncated, force a) closure", pos);
				sb+=REPLACED;
				sb+=")";//trick heuristic to force falling into the in() filter
			}
		}else if (ch=='-' && (pos+1<inputArray.size()) && inputArray.at(pos+1) =='-'){ //SINGLE LINE COMMENT CASE?
			pos = pos+2;
			//move cursor to end of line
			if (pos<inputArray.size()){
				while(pos < inputArray.size() && inputArray.at(pos) != '\n' ){
					pos++;
				}
			}
			// do not append to sb to clear comment
		} else if ((ch=='/' && (pos+1<inputArray.size()) && inputArray.at(pos+1) =='*')){ // MULTI LINE COMMENT CASE?
			pos = pos + 2;
			int openCommentCount = 1;
			try
			{
				while (openCommentCount > 0){ 
					if (inputArray.at(pos) == '*' && inputArray.at(pos+1) == '/' )
						openCommentCount--;
					if (inputArray.at(pos) == '/' && inputArray.at(pos+1) == '*' )
						openCommentCount++;						
					pos++;
				}
			}
			catch(...){
				logError(sqlString,"Parse multiline comment end of string", pos);
			}
			// do not append sb to clear comment					
		} else if (ch=='"') {// DELIMITER IDENTIFIER STARTING
			sb+='"';
			pos++;
			try{
				while(inputArray.at(pos) != '"' || ((pos+1<inputArray.size()) && inputArray.at(pos+1) == '"')){ //bypass the double double quote escape char
					if (inputArray.at(pos) == '"'){
						pos++;// skip one more if we are in escape sequence
						sb+='"';// but keep it in output
					}
					sb+=inputArray.at(pos);
					pos++;
				}
				sb+='"';
			}catch(...){
				logError(sqlString,"Parse multiline comment end of string", pos);
			}
		} else if (ch=='-' || ch=='+' ){ //VERIFY IF THIS IS A NUMERIC LITERAL SIGN and silence it if it is 
			if (isDDigit(inputArray.at(pos+1)) == 1){
				char precedingNonWhiteChar = getPrecedingNonWhiteSpaceOrTabChar(inputArray, pos);
				if (precedingNonWhiteChar == ')' || isValidIdentifierChar(precedingNonWhiteChar)) // - is operator
					sb+=ch; // so don t filter it out
			}else{
				sb+=ch;
			}
		}else{
			sb+=ch; // any other case just let echo to the result.
		}
			
		pos++; //process next character;
	}
 	//return  sb.toString().replaceAll("IN\\s*\\((?!\\s*SELECT).*a.*(,.*a.*)*\\)", "IN(a)"); // take care of IN(literal(,literal...))
	string retstr=SilenceINStatements(sb);		 
        char buf[128];
        memset(buf,0,128);
        boost::crc_32_type result;
        result.process_bytes(retstr.data(), retstr.length());
        long crcint = result.checksum();
        sprintf(buf,"%ld",crcint);
        retstr = buf;
        return retstr;
}
