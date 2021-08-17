exports.ensureEqual = (actual, expected) => {
	expect(actual).toBe(expected);
};
exports.ensureDeeplyEqual = (actual, expected) => {
	expect(actual).toEqual(expected);
};

exports.ensureArrayContains = (arr, elem) => {
	expect(arr).toContain(elem);
};

exports.ensureTruthy = predicate => {
	expect(predicate).toBeTruthy();
};

exports.ensureArrayHasObjectWithKeyValuePair = (arr, key, value) => {
	let found = false;
	for (const elem of arr) {
		if (Object.hasOwnProperty.call(elem, key) && elem[key] === value)
			found = true;
	}
	this.ensureTruthy(found);
};
