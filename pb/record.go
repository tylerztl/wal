/*
Copyright Zhigui.com. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pb

import "errors"

func (m *Record) Validate(crc uint32) error {
	if m.Crc == crc {
		return nil
	}
	m.Reset()
	return errors.New("record crc mismatch")
}
