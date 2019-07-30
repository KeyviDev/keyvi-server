// -*- mode: c++; tab-width: 4; indent-tabs-mode: t; c-file-style: "stroustrup"; -*-
// vi:set ts=4 sts=4 sw=4 noet :
// Copyright 2008, The TPIE development team
// 
// This file is part of TPIE.
// 
// TPIE is free software: you can redistribute it and/or modify it under
// the terms of the GNU Lesser General Public License as published by the
// Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.
// 
// TPIE is distributed in the hope that it will be useful, but WITHOUT ANY
// WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
// License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with TPIE.  If not, see <http://www.gnu.org/licenses/>

#ifndef _TPIE_BIT_H
#define _TPIE_BIT_H

// Get definitions for working with Unix and Windows
#include <portability.h>

#include <iostream>

namespace tpie {

// A bit with two operarators, addition (= XOR) and multiplication (=
// AND).
    class bit {

    private:
	char data;

    public:
	bit(void);
	bit(bool);
	bit(int);
	bit(long int);
	~bit(void);
	
	operator bool(void);
	operator int(void);
	operator long int(void);
	
	bit operator+=(bit rhs);
	bit operator*=(bit rhs);
	
	friend bit operator+(bit op1, bit op2);
	friend bit operator*(bit op1, bit op2);
	
	friend std::ostream &operator<<(std::ostream &s, bit b);
    };

}  //  tpie namespace

#endif // _TPIE_BIT_H 
